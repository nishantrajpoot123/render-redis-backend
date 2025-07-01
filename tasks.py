import os
import tempfile
import uuid
import re
from datetime import datetime, timedelta
import shutil
import pdfplumber
import pandas as pd
from werkzeug.utils import secure_filename
import PyPDF2
import logging
from celery import Celery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Celery
redis_url = os.getenv('REDIS_URL', 'redis://red-d1dov1mr433s73fkt63g:6379')
celery_app = Celery('sds_processor', broker=redis_url, backend=redis_url)

# Celery configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    result_expires=3600,  # Results expire after 1 hour
    task_time_limit=1800,  # 30 minutes max per task
    task_soft_time_limit=1500,  # 25 minutes soft limit
)

# Constants
UPLOAD_FOLDER = os.path.join(tempfile.gettempdir(), 'sds_uploads')
ALLOWED_EXTENSIONS_PDF = {'pdf'}
ALLOWED_EXTENSIONS_EXCEL = {'xlsx', 'xls'}

# Create directories if they don't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Required columns with CAS Number explicitly included
COLUMNS = [
    "Description",
    "CAS Number",
    "Physical state (solid/liquid/gas)",
    "Composition",
    "Static Hazard",
    "Vapour Pressure (in mmHg)",
    "at temp (in degC)",
    "Flash Point (°C)",
    "Flammable Limits by Volume (LEL, UEL)",
    "Melting Point (°C)",
    "Boiling Point (°C)",
    "Density (g/cc)",
    "Relative Vapour Density (Air = 1)",
    "Ignition Temperature (°C)",
    "Threshold Limit Value (ppm)",
    "Immediate Danger to Life in Humans",
    "Toxicological Info LD50 (mg/kg)",
    "Source of Information"
]

# Copy all your existing helper functions here
def extract_pdf_text_fallback(pdf_path):
    """Extract text from PDF using PyPDF2 as fallback"""
    try:
        text = ""
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text
    except Exception as e:
        logger.error(f"PyPDF2 extraction failed for {pdf_path}: {str(e)}")
        return ""

def extract_pdf_text(pdf_path):
    """Extract text from PDF file with fallback methods"""
    text = ""
    
    # Try pdfplumber first
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        
        if text.strip():
            logger.info(f"Successfully extracted text using pdfplumber from {os.path.basename(pdf_path)}")
            return text
    except Exception as e:
        logger.warning(f"pdfplumber failed for {pdf_path}: {str(e)}")
    
    # Fallback to PyPDF2
    text = extract_pdf_text_fallback(pdf_path)
    if text.strip():
        logger.info(f"Successfully extracted text using PyPDF2 from {os.path.basename(pdf_path)}")
        return text
    
    logger.error(f"All text extraction methods failed for {pdf_path}")
    return ""

def clean_numeric_value(value_str):
    """Clean and standardize numeric values"""
    if not value_str or value_str.lower() in ['nda', 'n/a', 'not available']:
        return "NDA"
    
    # Remove extra whitespace and common prefixes
    value_str = value_str.strip()
    value_str = re.sub(r'^[:\-\s]+', '', value_str)
    
    # Extract first number found
    number_match = re.search(r'([\d,]+[.,]?\d*)', value_str)
    if number_match:
        number = number_match.group(1)
        # Convert comma decimal separator to dot
        number = re.sub(r'(\d+),(\d+)$', r'\1.\2', number)
        return number
    
    return "NDA"

def extract_flammable_limits(text):
    """Extract flammable limits (LEL and UEL) with comprehensive pattern matching"""
    
    # Initialize variables
    lel_value = None
    uel_value = None
    
    # Comprehensive patterns for flammable limits
    flammable_patterns = [
        # Pattern 1: LEL and UEL on same line with percentages
        r"LEL[:\s]*(\d+(?:\.\d+)?)\s*%.*?UEL[:\s]*(\d+(?:\.\d+)?)\s*%",
        r"Lower\s+explosive\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%.*?Upper\s+explosive\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%",
        r"LFL[:\s]*(\d+(?:\.\d+)?)\s*%.*?UFL[:\s]*(\d+(?:\.\d+)?)\s*%",
        
        # Pattern 2: Range format like "2.1 - 12.8%" or "2.1% - 12.8%"
        r"(?:LEL|Lower\s+explosive\s+limit|LFL|Flammable\s+limits?)[:\s]*(\d+(?:\.\d+)?)\s*%?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%",
        r"Explosive\s+limits?[:\s]*(\d+(?:\.\d+)?)\s*%?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%",
        r"Flammability\s+limits?[:\s]*(\d+(?:\.\d+)?)\s*%?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%",
        
        # Pattern 3: Parentheses format like "(2.1 - 12.8%)" or "(LEL: 2.1%, UEL: 12.8%)"
        r"\(\s*(\d+(?:\.\d+)?)\s*%?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%?\s*\)",
        r"\(\s*LEL[:\s]*(\d+(?:\.\d+)?)\s*%.*?UEL[:\s]*(\d+(?:\.\d+)?)\s*%\s*\)",
        
        # Pattern 4: Table-like format with vol% or volume%
        r"(?:LEL|Lower)[:\s]*(\d+(?:\.\d+)?)\s*(?:vol\s*%|%\s*vol|%|volume\s*%).*?(?:UEL|Upper)[:\s]*(\d+(?:\.\d+)?)\s*(?:vol\s*%|%\s*vol|%|volume\s*%)",
        
        # Pattern 5: Simple numeric range without explicit LEL/UEL labels but in flammable context
        r"(?:Flammable|Explosive)\s+(?:range|limits?)[:\s]*(\d+(?:\.\d+)?)\s*[-–—]\s*(\d+(?:\.\d+)?)\s*%",
        
        # Pattern 6: Individual LEL/UEL on separate lines
        r"LEL[:\s]*(\d+(?:\.\d+)?)\s*%",
        r"UEL[:\s]*(\d+(?:\.\d+)?)\s*%",
    ]
    
    # Try patterns that capture both LEL and UEL
    for pattern in flammable_patterns[:5]:  # First 5 patterns capture both values
        matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if matches:
            match = matches[0]
            if len(match) == 2:
                lel_value = match[0].strip()
                uel_value = match[1].strip()
                logger.debug(f"Found LEL/UEL pair with pattern '{pattern}': LEL={lel_value}%, UEL={uel_value}%")
                break
    
    # If we didn't find a pair, try individual patterns
    if not lel_value or not uel_value:
        # Look for individual LEL
        lel_patterns = [
            r"LEL[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"Lower\s+explosive\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"LFL[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"Lower\s+flammable\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%"
        ]
        
        for pattern in lel_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                lel_value = matches[0].strip()
                logger.debug(f"Found individual LEL: {lel_value}%")
                break
        
        # Look for individual UEL
        uel_patterns = [
            r"UEL[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"Upper\s+explosive\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"UFL[:\s]*(\d+(?:\.\d+)?)\s*%",
            r"Upper\s+flammable\s+limit[:\s]*(\d+(?:\.\d+)?)\s*%"
        ]
        
        for pattern in uel_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                uel_value = matches[0].strip()
                logger.debug(f"Found individual UEL: {uel_value}%")
                break
    
    # Format the result
    if lel_value and uel_value:
        result = f"LEL: {lel_value}%, UEL: {uel_value}%"
    elif lel_value:
        result = f"LEL: {lel_value}%"
    elif uel_value:
        result = f"UEL: {uel_value}%"
    else:
        # Check for explicit "not applicable" or "non-flammable" statements
        non_flammable_patterns = [
            r"not\s+flammable",
            r"non[-\s]?flammable",
            r"flammable\s+limits?\s*:?\s*(?:not\s+applicable|n/?a)",
            r"explosive\s+limits?\s*:?\s*(?:not\s+applicable|n/?a)",
            r"does\s+not\s+burn",
            r"will\s+not\s+burn",
            r"non[-\s]?combustible"
        ]
        
        for pattern in non_flammable_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"Found non-flammable indicator: {pattern}")
                return "Non-flammable"
        
        logger.debug("No flammable limits found")
        result = "NDA"
    
    return result

def parse_sds_data(text, source_filename):
    """Enhanced SDS data extraction with comprehensive pattern matching and debugging"""
    logger.info(f"Parsing SDS data from {source_filename}")
    
    # Add debug logging
    logger.debug(f"Text length: {len(text)} characters")
    logger.debug(f"First 500 chars: {text[:500]}")
    
    def find_between(pattern, default="NDA", field_name=""):
        matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
        if matches:
            result = matches[0].strip() if isinstance(matches[0], str) else str(matches[0]).strip()
            result = clean_numeric_value(result) if any(char.isdigit() for char in result) else result
            logger.debug(f"Found {field_name}: {result}")
            return result
        logger.debug(f"No match found for {field_name}")
        return default
    
    flammable_limits = extract_flammable_limits(text)
    
    # Fixed CAS Number extraction - handles complete CAS number format
    def extract_cas_number(text):
        """Extract CAS number without applying numeric cleaning"""
        cas_patterns = [
            r"CAS-No\.?\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            r"CAS\s+No\.?\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            r"CAS\s+number\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            r"CAS#?\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            r"【CAS】\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            # More flexible pattern for various CAS formats
            r"(?:CAS|cas)(?:\s*-?\s*(?:No|NUMBER|#))?\s*[:\-]?\s*[\[\(]?\s*(\d{2,7}-\d{2}-\d)\s*[\]\)]?",
            # Pattern for standalone CAS numbers (more restrictive to avoid false positives)
            r"\b(\d{2,7}-\d{2}-\d)\b"
        ]
        
        for pattern in cas_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            if matches:
                cas_result = matches[0].strip()
                logger.debug(f"Found CAS Number with pattern '{pattern}': {cas_result}")
                return cas_result
        
        logger.debug("No CAS Number found")
        return "NDA"
    
    def extract_static_hazard(text):
        """Extract static hazard information - return Yes/No/NDA based on static discharge mentions"""
        # First check if there's any mention of static-related topics at all
        static_patterns = [
            r"static\s+discharge",
            r"electrostatic\s+discharge",
            r"static\s+electricity",
            r"electrostatic\s+charge",
            r"static\s+charge",
            r"precautionary\s+measures\s+against\s+static\s+discharge",
            r"measures\s+to\s+prevent.*static",
            r"ground.*bond.*container",
            r"grounding.*bonding",
            r"anti[-\s]?static",
            r"static\s+sensitive",
            r"electrostatic\s+ignition",
            r"static\s+buildup"
        ]
        
        # Patterns that indicate NO static hazard
        no_static_patterns = [
            r"no\s+static\s+hazard",
            r"static\s+hazard\s*:?\s*no",
            r"not\s+static\s+sensitive",
            r"no\s+electrostatic\s+hazard",
            r"static\s+discharge\s*:?\s*not\s+applicable",
            r"static\s+discharge\s*:?\s*n/?a"
        ]
        
        # Check for explicit "No" indicators first
        for pattern in no_static_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"Found explicit no static hazard indicator: {pattern}")
                return "No"
        
        # Check for "Yes" indicators
        for pattern in static_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.debug(f"Found static hazard indicator: {pattern}")
                return "Yes"
        
        # Check if there's any mention of handling/storage sections where static info might be expected
        handling_sections = [
            r"SECTION\s*7.*?(?:handling|storage)",
            r"handling\s+and\s+storage",
            r"precautions\s+for\s+safe\s+handling",
            r"storage\s+conditions"
        ]
        
        has_handling_section = False
        for pattern in handling_sections:
            if re.search(pattern, text, re.IGNORECASE):
                has_handling_section = True
                break
        
        if has_handling_section:
            # If there's a handling section but no static hazard info, assume "No"
            logger.debug("Found handling/storage section but no static hazard info - assuming No")
            return "No"
        else:
            # If no handling section found, data is not available
            logger.debug("No static hazard information or handling section found")
            return "NDA"
    
    cas_number = extract_cas_number(text)
    
    # Use PDF filename as description (remove .pdf extension)
    desc = os.path.splitext(source_filename)[0]
    
    # Enhanced pattern matching for various properties
    physical_state = find_between(r"Physical\s+state\s*:?\s*([^\n\r.]+)", "NDA", "Physical State")
    if physical_state == "NDA":
        physical_state = find_between(r"State\s*:?\s*([^\n\r.]+)", "NDA", "State")
    
    static_hazard = extract_static_hazard(text)
    
    # Vapor pressure with unit conversion
    vapour_pressure = "NDA"
    vapour_temp = "21"  # Default temperature
    
    # Try different vapor pressure patterns
    vp_patterns = [
        r"Vapou?r\s+pressure\s*:?\s*([\d,]+[.,]?\d*)\s*atm",
        r"Vapou?r\s+pressure\s*:?\s*([\d,]+[.,]?\d*)\s*mmHg",
        r"Vapou?r\s+pressure\s*:?\s*([\d,]+[.,]?\d*)\s*Pa",
        r"Pressure\s*:?\s*([\d,]+[.,]?\d*)\s*(?:atm|mmHg|Pa)"
    ]
    
    for pattern in vp_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = clean_numeric_value(match.group(1))
            if value != "NDA":
                if "atm" in pattern:
                    vapour_pressure = f"{float(value) * 760:.1f}"
                elif "Pa" in pattern:
                    vapour_pressure = f"{float(value) * 0.00750062:.1f}"
                else:
                    vapour_pressure = value
                break
    
    # Temperature for vapor pressure
    temp_match = re.search(r"(?:at|@)\s*(\d+)\s*°?C", text, re.IGNORECASE)
    if temp_match:
        vapour_temp = temp_match.group(1)
    
    # Extract other properties with multiple patterns
    flash_point = find_between(r"Flash\s+point\s*:?\s*([\d\-,]+[.,]?\d*)", "NDA", "Flash Point")
    melting_point = find_between(r"Melting\s+point\s*:?\s*([\d\-,]+[.,]?\d*)", "NDA", "Melting Point")
    boiling_point = find_between(r"Boiling\s+point\s*:?\s*([\d\-,]+[.,]?\d*)", "NDA", "Boiling Point")
    
    # Density with multiple units
    density_patterns = [
        r"Density.*?([0-9]+[,.]?[0-9]*)\s*(kg/m3|g/cm3|g/mL|g/L)",
        r"Density\s*:?\s*([\d,]+[.,]?\d*)\s*(?:g/cm³|g/cc|kg/m³)",
        r"Specific\s+gravity\s*:?\s*([\d,]+[.,]?\d*)"
    ]
    density = "NDA"
    for pattern in density_patterns:
        density = find_between(pattern, "NDA", "Density")
        if density != "NDA":
            break
    
    # LD50 extraction
    ld50_patterns = [
        r"LD50.*?([0-9,]+[.,]?\d*)\s*mg/kg",
        r"LD50\s*:?\s*([\d,]+[.,]?\d*)\s*mg/kg",
        r"LD50\s*:?\s*(?:oral|dermal)?\s*([\d,]+[.,]?\d*)\s*mg/kg",
        r"LD₅₀\s*:?\s*(?:oral|dermal)?\s*([\d,]+[.,]?\d*)\s*mg/kg"
    ]
    ld50 = "NDA"
    for pattern in ld50_patterns:
        ld50 = find_between(pattern, "NDA", "LD50")
        if ld50 != "NDA":
            break

    chemical_name_patterns = [
        r"Product name[:\s]*([^\n\r]+)",
        r"Product Name[:\s]*([^\n\r]+)", 
        r"PRODUCT NAME[:\s]*([^\n\r]+)",
        r"Product Name:[:\s]*([^\n\r]+)",
        r"Product name\s*:[:\s]*([^\n\r]+)",
        r"Identification of the substance[:\s]*([^\n\r]+)",
    ]

    name = "NDA"

    for pattern in chemical_name_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            extracted = match.group(1).strip()

            # ✅ Skip if it's clearly a section header (too long or contains slashes)
            if len(extracted) > 60 or "/" in extracted.lower() or "company" in extracted.lower():
                continue

            name = extracted
            break

    extracted_data = {
        "Description": desc,
        "CAS Number": cas_number,
        "Physical state (solid/liquid/gas)": physical_state,
        "Composition": name,
        "Static Hazard": static_hazard,
        "Vapour Pressure (in mmHg)": vapour_pressure,
        "at temp (in degC)": vapour_temp,
        "Flash Point (°C)": flash_point,
        "Flammable Limits by Volume (LEL, UEL)": extract_flammable_limits(text),
        "Melting Point (°C)": melting_point,
        "Boiling Point (°C)": boiling_point,
        "Density (g/cc)": density,
        "Relative Vapour Density (Air = 1)": find_between(r"Relative\s+vapou?r\s+density\s*:?\s*([\d,]+[.,]?\d*)", "NDA", "Vapor Density"),
        "Ignition Temperature (°C)": find_between(r"(?:Auto|Self)[-\s]?ignition\s+temperature\s*:?\s*([\d,]+[.,]?\d*)", "NDA", "Ignition Temp"),
        "Threshold Limit Value (ppm)": find_between(r"TLV\s*:?\s*([^\n\r]+)", "NDA", "TLV"),
        "Immediate Danger to Life in Humans": find_between(r"LC50\s*[-:]\s*.*?([0-9,]+.*?)\s*(mg|g|ppm|mL|L)", "NDA"),
        "Toxicological Info LD50 (mg/kg)": ld50,
        "Source of Information": "MSDS"
    }
    
    # Log extracted data for debugging
    logger.info(f"Extracted data for {source_filename}:")
    for key, value in extracted_data.items():
        if value != "NDA":
            logger.info(f"  {key}: {value}")
    
    return extracted_data

def merge_by_cas_number_optional(rows, merge_duplicates=False):
    """Optionally group SDS entries by CAS number and merge each group into a single row."""
    if not rows:
        return []
    
    if not merge_duplicates:
        logger.info(f"Keeping all {len(rows)} entries without merging")
        return rows
    
    merged = {}
    
    for row in rows:
        cas_key = row.get("CAS Number", "").strip()
        if cas_key.lower() in ["nda", "", "n/a"]:
            # For entries without CAS numbers, use description as key to avoid merging
            cas_key = f"no_cas_{row.get('Description', 'unknown')}_{len(merged)}"
        
        if cas_key not in merged:
            merged[cas_key] = row.copy()
        else:
            # Merge data, preferring non-NDA values
            for col in COLUMNS:
                current = merged[cas_key].get(col, "NDA")
                new = row.get(col, "NDA")
                if current in ["", "NDA", None, "n/a"] and new not in ["", "NDA", None, "n/a"]:
                    merged[cas_key][col] = new
    
    logger.info(f"Merged {len(rows)} entries into {len(merged)} unique entries")
    return list(merged.values())

def check_for_duplicates(existing_df, new_data_df, duplicate_check_mode="description"):
    """Check for duplicates based on different criteria."""
    if duplicate_check_mode == "none":
        return new_data_df
    
    if len(existing_df) == 0:
        return new_data_df
    
    # Create filters based on mode
    duplicate_filters = []
    
    if duplicate_check_mode in ["cas", "both"]:
        # Filter by CAS Number
        if "CAS Number" in existing_df.columns:
            existing_cas = set(existing_df["CAS Number"].dropna().astype(str).str.strip().str.lower())
            existing_cas.discard("nda")  # Remove NDA entries from duplicate check
            cas_filter = ~new_data_df["CAS Number"].str.strip().str.lower().isin(existing_cas)
            duplicate_filters.append(cas_filter)
    
    if duplicate_check_mode in ["description", "both"]:
        # Filter by Description
        if "Description" in existing_df.columns:
            existing_desc = set(existing_df["Description"].dropna().astype(str).str.strip().str.lower())
            desc_filter = ~new_data_df["Description"].str.strip().str.lower().isin(existing_desc)
            duplicate_filters.append(desc_filter)
    
    # Apply filters
    if duplicate_filters:
        if duplicate_check_mode == "both":
            # For "both" mode, entry must be new in BOTH CAS and description
            combined_filter = duplicate_filters[0] & duplicate_filters[1]
        else:
            # For single criteria, use that filter
            combined_filter = duplicate_filters[0]
        
        filtered_df = new_data_df[combined_filter]
        logger.info(f"Duplicate check ({duplicate_check_mode}): {len(new_data_df)} -> {len(filtered_df)} entries")
        return filtered_df
    
    return new_data_df

@celery_app.task(bind=True)
def process_sds_files(self, session_id, pdf_file_paths, excel_file_path, merge_duplicates, duplicate_check):
    """Celery task to process SDS files asynchronously"""
    try:
        # Update task state
        self.update_state(state='PROGRESS', meta={'status': 'Starting processing...', 'progress': 0})
        
        logger.info(f"Starting SDS processing for session {session_id}")
        logger.info(f"Processing {len(pdf_file_paths)} PDF files")
        logger.info(f"Processing options: merge_duplicates={merge_duplicates}, duplicate_check={duplicate_check}")
        
        # Process PDF files and extract SDS data
        all_data = []
        processed_files = 0
        skipped_files = []
        
        total_files = len(pdf_file_paths)
        
        for i, pdf_path in enumerate(pdf_file_paths):
            filename = os.path.basename(pdf_path)
            try:
                # Update progress
                progress = int((i / total_files) * 80)  # Reserve 20% for final processing
                self.update_state(
                    state='PROGRESS', 
                    meta={
                        'status': f'Processing {filename}...',
                        'progress': progress,
                        'current_file': filename,
                        'processed': i,
                        'total': total_files
                    }
                )
                
                logger.info(f"Processing {filename}...")
                text = extract_pdf_text(pdf_path)
                
                if text.strip():  # Only process if we got text
                    parsed_data = parse_sds_data(text, filename)
                    all_data.append(parsed_data)
                    processed_files += 1
                    logger.info(f"Successfully processed {filename}")
                else:
                    skipped_files.append(f"{filename} (no text extracted)")
                    logger.warning(f"No text extracted from {filename}")
                    
            except Exception as e:
                error_msg = f"Error processing {filename}: {str(e)}"
                logger.error(error_msg)
                skipped_files.append(f"{filename} (processing error: {str(e)})")
        
        if not all_data:
            return {
                'success': False,
                'error': 'No valid SDS data could be extracted from any PDF files. Please check if the PDFs contain readable text.'
            }
        
        # Update progress
        self.update_state(
            state='PROGRESS', 
            meta={
                'status': 'Processing extracted data...',
                'progress': 80
            }
        )
        
        logger.info(f"Extracted data from {processed_files} files")
        
        # Optionally merge data by CAS Number
        processed_data = merge_by_cas_number_optional(all_data, merge_duplicates)
        
        # Create DataFrame with proper column structure
        new_data_df = pd.DataFrame(processed_data)
        
        # Ensure all required columns exist
        for col in COLUMNS:
            if col not in new_data_df.columns:
                new_data_df[col] = "NDA"
        
        # Reorder columns
        new_data_df = new_data_df[COLUMNS]
        
        # Update progress
        self.update_state(
            state='PROGRESS', 
            meta={
                'status': 'Processing Excel file...',
                'progress': 85
            }
        )
        
        # Read existing Excel file
        try:
            existing_df = pd.read_excel(excel_file_path)
            logger.info(f"Read existing Excel with {len(existing_df)} rows")
            
            # Ensure existing DataFrame has all required columns
            for col in COLUMNS:
                if col not in existing_df.columns:
                    existing_df[col] = "NDA"
            
            # Reorder columns to match COLUMNS order
            existing_df = existing_df.reindex(columns=COLUMNS, fill_value="NDA")
            
            # Check for duplicates based on specified criteria
            new_entries = check_for_duplicates(existing_df, new_data_df, duplicate_check)
            
            # Combine existing and new data
            if len(new_entries) > 0:
                combined_df = pd.concat([existing_df, new_entries], ignore_index=True)
            else:
                combined_df = existing_df
                
        except Exception as e:
            logger.error(f"Error reading Excel file: {str(e)}")
            # If we can't read the existing file, just use the new data
            combined_df = new_data_df
            new_entries = new_data_df
        
        # Update progress
        self.update_state(
            state='PROGRESS', 
            meta={
                'status': 'Saving results...',
                'progress': 95
            }
        )
        
        # Save to new Excel file
        # Fix for the download issue in your tasks.py
        # Replace the file saving section in your process_sds_files task (around line 350-360)
        
        # Save to session directory instead of /tmp
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        output_filename = f"sds_extraction_results.xlsx"
        output_path = os.path.join(session_dir, output_filename)
        
        # Ensure session directory exists
        os.makedirs(session_dir, exist_ok=True)
        
        # Save with proper formatting
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            combined_df.to_excel(writer, index=False, sheet_name='SDS_Data')
            
        logger.info(f"Saved updated Excel file to session directory: {output_path}")
        
        # DON'T clean up session directory immediately - keep it for download
        # Remove or comment out this section:
        # try:
        #     session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        #     if os.path.exists(session_dir):
        #         shutil.rmtree(session_dir, ignore_errors=True)
        #         logger.info(f"Cleaned up session directory: {session_id}")
        # except Exception as e:
        #     logger.warning(f"Failed to clean up session directory {session_id}: {str(e)}")
        
        # Update the result to include the correct filename
        result = {
            'success': True,
            'message': message,
            'outputFile': output_filename,  # This should match what's in the session directory
            'sessionId': session_id,
            'downloadUrl': f'/api/download/{session_id}',  # Add direct download URL
            'processedFiles': processed_files,
            'totalFiles': len(pdf_file_paths),
            'newEntriesAdded': len(new_entries) if 'new_entries' in locals() else len(new_data_df),
            'totalEntriesInOutput': len(combined_df),
            'processingOptions': {
                'mergeDuplicates': merge_duplicates,
                'duplicateCheck': duplicate_check
            }
        }
        
        if skipped_files:
            result['skippedFiles'] = skipped_files
        
        # Clean up uploaded files
        try:
            session_dir = os.path.join(UPLOAD_FOLDER, session_id)
            if os.path.exists(session_dir):
                shutil.rmtree(session_dir, ignore_errors=True)
                logger.info(f"Cleaned up session directory: {session_id}")
        except Exception as e:
            logger.warning(f"Failed to clean up session directory {session_id}: {str(e)}")
        
        return result
        
    except Exception as e:
        error_msg = f"Task processing error: {str(e)}"
        logger.error(error_msg)
        
        # Clean up on error
        try:
            session_dir = os.path.join(UPLOAD_FOLDER, session_id)
            if os.path.exists(session_dir):
                shutil.rmtree(session_dir, ignore_errors=True)
        except:
            pass
            
        return {
            'success': False,
            'error': error_msg
        }

@celery_app.task
def cleanup_old_files():
    """Celery task to clean up old files"""
    try:
        # Remove files older than 24 hours
        cutoff_time = datetime.now() - timedelta(hours=24)
        cleaned_sessions = 0
        cleaned_files = 0
        
        # Clean up upload folder
        if os.path.exists(UPLOAD_FOLDER):
            for session_id in os.listdir(UPLOAD_FOLDER):
                session_dir = os.path.join(UPLOAD_FOLDER, session_id)
                if os.path.isdir(session_dir):
                    try:
                        created_time = datetime.fromtimestamp(os.path.getctime(session_dir))
                        if created_time < cutoff_time:
                            shutil.rmtree(session_dir, ignore_errors=True)
                            cleaned_sessions += 1
                    except Exception as e:
                        logger.error(f"Error cleaning session {session_id}: {str(e)}")
        
        # Clean up processed folder
        tmp_dir = '/tmp'
        if os.path.exists(tmp_dir):
            for filename in os.listdir(tmp_dir):
                if filename.startswith('extracted_msds_'):
                    file_path = os.path.join(tmp_dir, filename)
                    try:
                        created_time = datetime.fromtimestamp(os.path.getctime(file_path))
                        if created_time < cutoff_time:
                            os.remove(file_path)
                            cleaned_files += 1
                    except Exception as e:
                        logger.error(f"Error cleaning file {filename}: {str(e)}")
        
        return {
            'success': True, 
            'message': f'Cleanup completed: {cleaned_sessions} sessions and {cleaned_files} files removed'
        }
    
    except Exception as e:
        return {'success': False, 'error': f'Error during cleanup: {str(e)}'}
