import os
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import logging
from dotenv import load_dotenv
from pathlib import Path
import traceback

# Load environment variables
load_dotenv()

# Configure enhanced logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["https://msds-sigma.vercel.app", "http://localhost:3000"]}})

# Configuration
UPLOAD_FOLDER = '/tmp/sds_uploads'
PROCESSED_FOLDER = '/tmp/sds_processed'  # Separate folder for processed files
ALLOWED_EXTENSIONS_PDF = {'pdf'}
ALLOWED_EXTENSIONS_EXCEL = {'xlsx', 'xls'}

# Create directories if they don't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

# Import Celery tasks with error handling
try:
    from tasks import celery_app, process_sds_files, cleanup_old_files
    CELERY_AVAILABLE = True
    logger.info("✅ Celery tasks imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import Celery tasks: {e}")
    CELERY_AVAILABLE = False

def allowed_file(filename, allowed_extensions):
    """Check if file has allowed extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

def log_file_info(file_path, description):
    """Log detailed file information for debugging"""
    try:
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            file_stat = os.stat(file_path)
            logger.info(f"📄 {description}: {file_path}")
            logger.info(f"   Size: {file_size} bytes")
            logger.info(f"   Modified: {datetime.fromtimestamp(file_stat.st_mtime)}")
            logger.info(f"   Readable: {os.access(file_path, os.R_OK)}")
            return True
        else:
            logger.error(f"❌ {description} does not exist: {file_path}")
            return False
    except Exception as e:
        logger.error(f"❌ Error checking file {file_path}: {e}")
        return False

@app.route('/')
def index():
    return jsonify({
        'message': 'SDS Processing API Server with Enhanced Debugging',
        'status': 'running',
        'version': '3.1',
        'celery_available': CELERY_AVAILABLE,
        'folders': {
            'upload': UPLOAD_FOLDER,
            'processed': PROCESSED_FOLDER
        },
        'endpoints': {
            'upload': 'POST /api/upload',
            'status': 'GET /api/status/<task_id>',
            'download': 'GET /api/download/<filename>',
            'files': 'GET /api/files/<session_id>',
            'cleanup': 'POST /api/cleanup',
            'health': 'GET /api/health'
        }
    })

@app.route('/api/upload', methods=['POST', 'OPTIONS'])
def upload_files():
    if request.method == 'OPTIONS':
        return '', 200

    session_id = None
    session_dir = None
    
    try:
        logger.info("🚀 Processing upload request")
        
        # Check if Celery is available
        if not CELERY_AVAILABLE:
            return jsonify({'error': 'Celery task system not available'}), 500
        
        if 'pdfFiles' not in request.files or 'excelFile' not in request.files:
            logger.error("❌ Missing required files")
            return jsonify({'error': 'Missing required files (pdfFiles or excelFile)'}), 400
        
        pdf_files = request.files.getlist('pdfFiles')
        excel_file = request.files['excelFile']
        
        # Get processing options from form data
        merge_duplicates = request.form.get('mergeDuplicates', 'false').lower() == 'true'
        duplicate_check = request.form.get('duplicateCheck', 'none')
        
        logger.info(f"⚙️ Processing options: merge_duplicates={merge_duplicates}, duplicate_check={duplicate_check}")
        
        if not pdf_files or excel_file.filename == '':
            logger.error("❌ No files selected")
            return jsonify({'error': 'No files selected'}), 400
        
        logger.info(f"📁 Received {len(pdf_files)} PDF files and 1 Excel file")
        
        # Validate file extensions
        for pdf_file in pdf_files:
            if not allowed_file(pdf_file.filename, ALLOWED_EXTENSIONS_PDF):
                logger.error(f"❌ Invalid PDF file format: {pdf_file.filename}")
                return jsonify({'error': f'Invalid PDF file format: {pdf_file.filename}'}), 400
        
        if not allowed_file(excel_file.filename, ALLOWED_EXTENSIONS_EXCEL):
            logger.error(f"❌ Invalid Excel file format: {excel_file.filename}")
            return jsonify({'error': 'Invalid Excel file format'}), 400
        
        # Create unique session ID for this upload
        session_id = str(uuid.uuid4())
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        processed_dir = os.path.join(PROCESSED_FOLDER, session_id)
        
        os.makedirs(session_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        logger.info(f"📂 Created session directories:")
        logger.info(f"   Upload: {session_dir}")
        logger.info(f"   Processed: {processed_dir}")
        
        # Save uploaded files with detailed logging
        pdf_paths = []
        total_pdf_size = 0
        
        for i, pdf_file in enumerate(pdf_files):
            pdf_filename = secure_filename(pdf_file.filename)
            pdf_path = os.path.join(session_dir, pdf_filename)
            
            logger.info(f"💾 Saving PDF {i+1}/{len(pdf_files)}: {pdf_filename}")
            pdf_file.save(pdf_path)
            
            # Verify file was saved correctly
            if log_file_info(pdf_path, f"Saved PDF {i+1}"):
                pdf_paths.append(pdf_path)
                total_pdf_size += os.path.getsize(pdf_path)
            else:
                raise Exception(f"Failed to save PDF file: {pdf_filename}")
        
        # Save Excel file
        excel_filename = secure_filename(excel_file.filename)
        excel_path = os.path.join(session_dir, excel_filename)
        
        logger.info(f"💾 Saving Excel file: {excel_filename}")
        excel_file.save(excel_path)
        
        if not log_file_info(excel_path, "Saved Excel file"):
            raise Exception(f"Failed to save Excel file: {excel_filename}")
        
        logger.info(f"📊 Upload summary:")
        logger.info(f"   Total PDF size: {total_pdf_size / 1024:.2f} KB")
        logger.info(f"   Excel size: {os.path.getsize(excel_path) / 1024:.2f} KB")
        
        # Replace the task_params section in your upload_files() function with this:

        # Start Celery task with correct parameters
        task_params = {
            'session_id': session_id,
            'pdf_file_paths': pdf_paths,
            'excel_file_path': excel_path,
            # Remove 'processed_dir' - the task should construct this path internally
            'merge_duplicates': merge_duplicates,
            'duplicate_check': duplicate_check
        }
        
        logger.info(f"🎯 Starting Celery task with parameters:")
        for key, value in task_params.items():
            if key != 'pdf_file_paths':  # Don't log the full path list
                logger.info(f"   {key}: {value}")
        
        task = process_sds_files.delay(**task_params)

        
        logger.info(f"✅ Started Celery task {task.id} for session {session_id}")
        
        return jsonify({
            'success': True,
            'message': 'Files uploaded successfully. Processing started.',
            'taskId': task.id,
            'sessionId': session_id,
            'totalFiles': len(pdf_files),
            'uploadSummary': {
                'pdfCount': len(pdf_files),
                'totalPdfSize': total_pdf_size,
                'excelSize': os.path.getsize(excel_path),
                'sessionDir': session_dir,
                'processedDir': processed_dir
            },
            'processingOptions': {
                'mergeDuplicates': merge_duplicates,
                'duplicateCheck': duplicate_check
            }
        })
    
    except Exception as e:
        error_msg = f"Upload processing error: {str(e)}"
        logger.error(f"❌ {error_msg}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        
        # Cleanup on error
        if session_dir and os.path.exists(session_dir):
            try:
                import shutil
                shutil.rmtree(session_dir)
                logger.info(f"🧹 Cleaned up session directory after error: {session_dir}")
            except Exception as cleanup_error:
                logger.error(f"❌ Failed to cleanup session directory: {cleanup_error}")
        
        return jsonify({'error': error_msg, 'traceback': traceback.format_exc()}), 500

@app.route('/api/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """Get the status of a Celery task with enhanced debugging"""
    try:
        if not CELERY_AVAILABLE:
            return jsonify({'error': 'Celery not available'}), 500
        
        task = celery_app.AsyncResult(task_id)
        logger.debug(f"🔍 Checking status for task {task_id}: {task.state}")
        
        if task.state == 'PENDING':
            response = {
                'state': task.state,
                'status': 'Task is waiting to be processed...',
                'progress': 0
            }
        elif task.state == 'PROGRESS':
            info = task.info or {}
            response = {
                'state': task.state,
                'status': info.get('status', ''),
                'progress': info.get('progress', 0),
                'current_file': info.get('current_file', ''),
                'processed': info.get('processed', 0),
                'total': info.get('total', 0),
                'debug_info': info.get('debug_info', {})
            }
        elif task.state == 'SUCCESS':
            result = task.result or {}
            response = {
                'state': task.state,
                'result': result,
                'progress': 100,
                'output_file': result.get('output_file'),
                'processed_count': result.get('processed_count', 0),
                'debug_info': result.get('debug_info', {})
            }
            
            # Verify output file exists
            if result.get('output_file'):
                output_exists = os.path.exists(result['output_file'])
                response['output_file_exists'] = output_exists
                if output_exists:
                    response['output_file_size'] = os.path.getsize(result['output_file'])
                logger.info(f"📄 Output file check: {result['output_file']} exists={output_exists}")
                
        else:  # FAILURE
            error_info = task.info
            response = {
                'state': task.state,
                'error': str(error_info),
                'progress': 0,
                'traceback': getattr(error_info, 'traceback', None) if hasattr(error_info, 'traceback') else None
            }
            logger.error(f"❌ Task {task_id} failed: {error_info}")
        
        return jsonify(response)
    
    except Exception as e:
        error_msg = f'Error getting task status: {str(e)}'
        logger.error(f"❌ {error_msg}")
        return jsonify({
            'state': 'ERROR',
            'error': error_msg,
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/files/<session_id>', methods=['GET'])
def list_session_files(session_id):
    """List all files in a session for debugging"""
    try:
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        processed_dir = os.path.join(PROCESSED_FOLDER, session_id)
        
        files_info = {
            'session_id': session_id,
            'upload_dir': session_dir,
            'processed_dir': processed_dir,
            'upload_files': [],
            'processed_files': []
        }
        
        # List upload files
        if os.path.exists(session_dir):
            for filename in os.listdir(session_dir):
                file_path = os.path.join(session_dir, filename)
                if os.path.isfile(file_path):
                    files_info['upload_files'].append({
                        'name': filename,
                        'path': file_path,
                        'size': os.path.getsize(file_path),
                        'modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                    })
        
        # List processed files
        if os.path.exists(processed_dir):
            for filename in os.listdir(processed_dir):
                file_path = os.path.join(processed_dir, filename)
                if os.path.isfile(file_path):
                    files_info['processed_files'].append({
                        'name': filename,
                        'path': file_path,
                        'size': os.path.getsize(file_path),
                        'modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                    })
        
        return jsonify(files_info)
    
    except Exception as e:
        logger.error(f"❌ Error listing session files: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    """Enhanced download with multiple location search"""
    try:
        secure_fname = secure_filename(filename)
        
        # Search in multiple possible locations
        search_paths = [
            os.path.join("/tmp", secure_fname),
            os.path.join(PROCESSED_FOLDER, secure_fname),
            os.path.join(UPLOAD_FOLDER, secure_fname)
        ]
        
        # Also search in session subdirectories
        session_id = request.args.get('session_id')
        if session_id:
            search_paths.extend([
                os.path.join(PROCESSED_FOLDER, session_id, secure_fname),
                os.path.join(UPLOAD_FOLDER, session_id, secure_fname)
            ])
        
        logger.info(f"🔍 Searching for file: {secure_fname}")
        for path in search_paths:
            logger.debug(f"   Checking: {path}")
            if os.path.exists(path) and os.path.isfile(path):
                logger.info(f"✅ Found file at: {path}")
                log_file_info(path, "Download file")
                return send_file(path, as_attachment=True)
        
        logger.error(f"❌ File not found in any location: {secure_fname}")
        return jsonify({
            'error': 'File not found',
            'searched_paths': search_paths,
            'filename': secure_fname
        }), 404
        
    except Exception as e:
        logger.error(f"❌ Error downloading file: {e}")
        return jsonify({'error': f'Error downloading file: {str(e)}'}), 500

@app.route('/api/cleanup', methods=['POST'])
def cleanup_old_files_endpoint():
    """Trigger cleanup task"""
    try:
        if not CELERY_AVAILABLE:
            return jsonify({'error': 'Celery not available'}), 500
            
        task = cleanup_old_files.delay()
        logger.info(f"🧹 Started cleanup task: {task.id}")
        return jsonify({
            'success': True,
            'message': 'Cleanup task started',
            'taskId': task.id
        })
    except Exception as e:
        logger.error(f"❌ Error starting cleanup task: {e}")
        return jsonify({'error': f'Error starting cleanup task: {str(e)}'}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Enhanced health check endpoint"""
    try:
        health_info = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'version': '3.1',
            'folders': {
                'upload': UPLOAD_FOLDER,
                'processed': PROCESSED_FOLDER,
                'upload_exists': os.path.exists(UPLOAD_FOLDER),
                'processed_exists': os.path.exists(PROCESSED_FOLDER)
            },
            'celery': {
                'available': CELERY_AVAILABLE,
                'workers': 0
            }
        }
        
        # Check folder permissions
        for folder_name, folder_path in [('upload', UPLOAD_FOLDER), ('processed', PROCESSED_FOLDER)]:
            health_info['folders'][f'{folder_name}_writable'] = os.access(folder_path, os.W_OK)
            health_info['folders'][f'{folder_name}_readable'] = os.access(folder_path, os.R_OK)
        
        # Check Celery connection if available
        if CELERY_AVAILABLE:
            try:
                inspect = celery_app.control.inspect()
                active_workers = inspect.active()
                health_info['celery']['workers'] = len(active_workers) if active_workers else 0
                health_info['celery']['redis_url'] = os.getenv('REDIS_URL', 'redis://red-d1dov1mr433s73fkt63g:6379')
            except Exception as celery_error:
                health_info['celery']['error'] = str(celery_error)
                health_info['status'] = 'degraded'
        
        return jsonify(health_info)
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    print("🚀 Starting Enhanced SDS Processing Flask Server v3.1...")
    print(f"📁 Upload folder: {UPLOAD_FOLDER}")
    print(f"📁 Processed folder: {PROCESSED_FOLDER}")
    print("🌐 Server will be available at http://localhost:5000")
    print(f"⚙️  Celery integration: {'✅ Enabled' if CELERY_AVAILABLE else '❌ Disabled'}")
    print("🔍 Enhanced debugging and logging enabled")
    print("🎛️  Processing options:")
    print("   - mergeDuplicates: Merge entries with same CAS number")
    print("   - duplicateCheck: none|cas|description|both")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
