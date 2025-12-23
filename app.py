import os
import subprocess
import pandas as pd
from flask import Flask, render_template, request, send_file, redirect, url_for
from werkzeug.utils import secure_filename
import logging

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'
app.config['ALLOWED_EXTENSIONS'] = {'xlsx'}
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Create directories if they don't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def run_treport_queue(tracking_file, affiliation_file, min_confidence=78):
    """Run the treport_queue.py script with given parameters"""
    cmd = [
        'python', 'treport_queue.py',
        tracking_file,
        affiliation_file,
        '--min-confidence', str(min_confidence),
        '-o', os.path.join(app.config['PROCESSED_FOLDER'], 'processed_with_queue.xlsx')
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr

def run_time_report(input_file):
    """Run the time_report.py script"""
    output_file = os.path.join(app.config['PROCESSED_FOLDER'], 'final_report.xlsx')
    cmd = [
        'python', 'time_report.py',
        input_file,
        output_file
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0, output_file, result.stdout, result.stderr

@app.route('/', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'POST':
        # Check if files are present
        if 'tracking_file' not in request.files or 'affiliation_file' not in request.files:
            return redirect(request.url)
        
        tracking_file = request.files['tracking_file']
        affiliation_file = request.files['affiliation_file']
        min_confidence = request.form.get('min_confidence', 78, type=float)
        
        if tracking_file.filename == '' or affiliation_file.filename == '':
            return redirect(request.url)
        
        if tracking_file and allowed_file(tracking_file.filename) and \
           affiliation_file and allowed_file(affiliation_file.filename):
            
            # Save uploaded files
            tracking_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(tracking_file.filename))
            affiliation_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(affiliation_file.filename))
            
            tracking_file.save(tracking_path)
            affiliation_file.save(affiliation_path)
            
            # Run treport_queue.py
            success, stdout, stderr = run_treport_queue(tracking_path, affiliation_path, min_confidence)
            if not success:
                return f"Error processing files: {stderr}", 500
            
            # Run time_report.py on the processed file
            processed_file = os.path.join(app.config['PROCESSED_FOLDER'], 'processed_with_queue.xlsx')
            success, output_file, time_stdout, time_stderr = run_time_report(processed_file)
            if not success:
                return f"Error generating report: {time_stderr}", 500
            
            # Read the final report for display
            try:
                # Read the Agent Summary sheet
                agent_summary_df = pd.read_excel(output_file, sheet_name='Agent Summary')
                
                # Read the Category Summary sheet
                category_summary_df = pd.read_excel(output_file, sheet_name='Category Summary')
                
                # Convert to HTML for display
                agent_summary_html = agent_summary_df.to_html(classes='table table-striped', index=False)
                category_summary_html = category_summary_df.to_html(classes='table table-striped', index=False)
                
                output_filename = os.path.basename(output_file)
                return render_template('upload.html',
                                     agent_summary=agent_summary_html,
                                     category_summary=category_summary_html,
                                     output_filename=output_filename)
            
            except Exception as e:
                return f"Error reading output file: {str(e)}", 500
    
    return render_template('upload.html')

@app.route('/download/<filename>')
def download_file(filename):
    return send_file(os.path.join(app.config['PROCESSED_FOLDER'], filename), as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
