import os
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import logging
from dotenv import load_dotenv
from tasks import celery_app, process_sds_files, cleanup_old_files

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["https://extractmsds.vercel.app", "http://localhost:3000"]}})

# Configuration
UPLOAD_FOLDER = '/tmp/sds_uploads'
ALLOWED_EXTENSIONS_PDF = {'pdf'}
ALLOWED_EXTENSIONS_EXCEL = {'xlsx', 'xls'}

# Create directories if they don't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename, allowed_extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

@app.route('/')
def index():
    return jsonify({
        'message': 'SDS Processing API Server with Celery',
        'status': 'running',
        'version': '3.0',
        'endpoints': {
            'upload': 'POST /api/upload',
            'status': 'GET /api/status/<task_id>',
            'download': 'GET /api/download/<filename>',
            'cleanup': 'POST /api/cleanup',
            'health': 'GET /api/health'
        }
    })

@app.route('/api/upload', methods=['POST', 'OPTIONS'])
def upload_files():
    if request.method == 'OPTIONS':
        return '', 200  # This handles the CORS preflight request

    try:
        logger.info("Processing upload request")
        
        if 'pdfFiles' not in request.files or 'excelFile' not in request.files:
            return jsonify({'error': 'Missing required files (pdfFiles or excelFile)'}), 400
        
        pdf_files = request.files.getlist('pdfFiles')
        excel_file = request.files['excelFile']
        
        # Get processing options from form data
        merge_duplicates = request.form.get('mergeDuplicates', 'false').lower() == 'true'
        duplicate_check = request.form.get('duplicateCheck', 'none')  # none, cas, description, both
        
        logger.info(f"Processing options: merge_duplicates={merge_duplicates}, duplicate_check={duplicate_check}")
        
        if not pdf_files or excel_file.filename == '':
            return jsonify({'error': 'No files selected'}), 400
        
        logger.info(f"Received {len(pdf_files)} PDF files and 1 Excel file")
        
        # Validate file extensions
        for pdf_file in pdf_files:
            if not allowed_file(pdf_file.filename, ALLOWED_EXTENSIONS_PDF):
                return jsonify({'error': f'Invalid PDF file format: {pdf_file.filename}'}), 400
        
        if not allowed_file(excel_file.filename, ALLOWED_EXTENSIONS_EXCEL):
            return jsonify({'error': 'Invalid Excel file format'}), 400
        
        # Create unique session ID for this upload
        session_id = str(uuid.uuid4())
        session_dir = os.path.join(UPLOAD_FOLDER, session_id)
        os.makedirs(session_dir, exist_ok=True)
        
        # Save uploaded files
        pdf_paths = []
        for pdf_file in pdf_files:
            pdf_path = os.path.join(session_dir, secure_filename(pdf_file.filename))
            pdf_file.save(pdf_path)
            pdf_paths.append(pdf_path)
            logger.info(f"Saved PDF: {pdf_file.filename}")
        
        excel_path = os.path.join(session_dir, secure_filename(excel_file.filename))
        excel_file.save(excel_path)
        logger.info(f"Saved Excel: {excel_file.filename}")
        
        # Start Celery task
        task = process_sds_files.delay(
            session_id=session_id,
            pdf_file_paths=pdf_paths,
            excel_file_path=excel_path,
            merge_duplicates=merge_duplicates,
            duplicate_check=duplicate_check
        )
        
        logger.info(f"Started Celery task {task.id} for session {session_id}")
        
        return jsonify({
            'success': True,
            'message': 'Files uploaded successfully. Processing started.',
            'taskId': task.id,
            'sessionId': session_id,
            'totalFiles': len(pdf_files),
            'processingOptions': {
                'mergeDuplicates': merge_duplicates,
                'duplicateCheck': duplicate_check
            }
        })
    
    except Exception as e:
        error_msg = f"Upload processing error: {str(e)}"
        logger.error(error_msg)
        return jsonify({'error': error_msg}), 500

@app.route('/api/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """Get the status of a Celery task"""
    try:
        task = celery_app.AsyncResult(task_id)
        
        if task.state == 'PENDING':
            response = {
                'state': task.state,
                'status': 'Task is waiting to be processed...',
                'progress': 0
            }
        elif task.state == 'PROGRESS':
            response = {
                'state': task.state,
                'status': task.info.get('status', ''),
                'progress': task.info.get('progress', 0),
                'current_file': task.info.get('current_file', ''),
                'processed': task.info.get('processed', 0),
                'total': task.info.get('total', 0)
            }
        elif task.state == 'SUCCESS':
            response = {
                'state': task.state,
                'result': task.result,
                'progress': 100
            }
        else:  # FAILURE
            response = {
                'state': task.state,
                'error': str(task.info),
                'progress': 0
            }
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({
            'state': 'ERROR',
            'error': f'Error getting task status: {str(e)}'
        }), 500

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    try:
        file_path = os.path.join("/tmp", secure_filename(filename))
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True)
        else:
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        return jsonify({'error': f'Error downloading file: {str(e)}'}), 500

@app.route('/api/cleanup', methods=['POST'])
def cleanup_old_files_endpoint():
    """Trigger cleanup task"""
    try:
        task = cleanup_old_files.delay()
        return jsonify({
            'success': True,
            'message': 'Cleanup task started',
            'taskId': task.id
        })
    except Exception as e:
        return jsonify({'error': f'Error starting cleanup task: {str(e)}'}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check Celery connection
        inspect = celery_app.control.inspect()
        active_workers = inspect.active()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'upload_folder': UPLOAD_FOLDER,
            'version': '3.0',
            'celery_workers': len(active_workers) if active_workers else 0,
            'redis_url': os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    print("🚀 Starting Enhanced SDS Processing Flask Server v3.0 with Celery...")
    print(f"📁 Upload folder: {UPLOAD_FOLDER}")
    print("🌐 Server will be available at http://localhost:5000")
    print("⚙️  Celery integration enabled")
    print("🎛️  Processing options:")
    print("   - mergeDuplicates: Merge entries with same CAS number")
    print("   - duplicateCheck: none|cas|description|both")
    app.run(debug=True, host='0.0.0.0', port=5000)
