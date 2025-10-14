from flask import Flask
import threading
import time
import os

app = Flask(__name__)

# Global variable to control your background process
is_running = False
thread = None

def background_task():
    """Your main code logic — runs continuously when started"""
    global is_running
    while is_running:
        # Replace this with your real code
        print("Running market logic...")
        time.sleep(5)  # simulate delay

@app.route('/start')
def start():
    global is_running, thread
    if not is_running:
        is_running = True
        thread = threading.Thread(target=background_task)
        thread.start()
        return "✅ Script started successfully!"
    else:
        return "⚠️ Script is already running!"

@app.route('/stop')
def stop():
    global is_running
    if is_running:
        is_running = False
        return "🛑 Script stopped."
    else:
        return "⚠️ Script is not running."

@app.route('/')
def home():
    return "🌐 Use /start or /stop to control your script."

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
