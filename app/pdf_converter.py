#!/usr/bin/env python3
"""PDF Converter - Windows GUI Application"""

import os
import sys
import json
import threading
import subprocess
import shutil
import time
from pathlib import Path
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

class Config:
    def __init__(self):
        self.repo_dir = Path(__file__).parent.parent
        self.input_dir = self.repo_dir / "input"
        self.output_dir = self.repo_dir / "output"
        self.logs_dir = self.repo_dir / "logs"
        self.check_interval = 15

class PDFConverterApp:
    def __init__(self, root):
        self.root = root
        self.config = Config()
        self.selected_files = []
        self.processing = False
        self.current_job_files = []
        self.setup_styles()
        self.setup_ui()
        self.show_welcome()

    def setup_styles(self):
        style = ttk.Style()
        style.configure("Title.TLabel", font=("Segoe UI", 16, "bold"))
        style.configure("Status.TLabel", font=("Segoe UI", 10))
        style.configure("Big.TButton", font=("Segoe UI", 11), padding=10)

    def setup_ui(self):
        self.root.title("PDF to Markdown Converter")
        self.root.geometry("800x700")
        self.root.minsize(600, 500)
        main_frame = ttk.Frame(self.root, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        title = ttk.Label(main_frame, text="PDF to Markdown Converter", style="Title.TLabel")
        title.pack(pady=(0, 20))
        file_frame = ttk.LabelFrame(main_frame, text="Select PDF Files", padding=15)
        file_frame.pack(fill=tk.X, pady=(0, 15))
        btn_frame = ttk.Frame(file_frame)
        btn_frame.pack(fill=tk.X)
        self.btn_select = ttk.Button(btn_frame, text="Select Files", command=self.select_files)
        self.btn_select.pack(side=tk.LEFT, padx=(0, 10))
        self.btn_clear = ttk.Button(btn_frame, text="Clear", command=self.clear_files)
        self.btn_clear.pack(side=tk.LEFT)
        self.file_listbox = tk.Listbox(file_frame, height=4, font=("Segoe UI", 10))
        self.file_listbox.pack(fill=tk.X, pady=(10, 0))
        self.btn_convert = ttk.Button(main_frame, text="Start Conversion (Upload to GitHub)", style="Big.TButton", command=self.start_conversion)
        self.btn_convert.pack(pady=15)
        self.progress = ttk.Progressbar(main_frame, mode="indeterminate", length=400)
        self.progress.pack(pady=(0, 10))
        self.status_var = tk.StringVar(value="Ready...")
        self.status_label = ttk.Label(main_frame, textvariable=self.status_var, style="Status.TLabel")
        self.status_label.pack(pady=(0, 15))
        result_frame = ttk.LabelFrame(main_frame, text="Results", padding=10)
        result_frame.pack(fill=tk.BOTH, expand=True)
        self.result_text = scrolledtext.ScrolledText(result_frame, wrap=tk.WORD, font=("Consolas", 10), height=15)
        self.result_text.pack(fill=tk.BOTH, expand=True)
        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.pack(fill=tk.X, pady=(15, 0))
        self.btn_open_output = ttk.Button(bottom_frame, text="Open Output Folder", command=self.open_output_folder)
        self.btn_open_output.pack(side=tk.LEFT)
        self.btn_clear_log = ttk.Button(bottom_frame, text="Clear Log", command=self.clear_log)
        self.btn_clear_log.pack(side=tk.LEFT, padx=10)

    def show_welcome(self):
        self.result_text.insert(tk.END, "Welcome to PDF Converter!\n\n")
        self.result_text.insert(tk.END, "How to use:\n")
        self.result_text.insert(tk.END, "1. Click 'Select Files' to choose PDF files\n")
        self.result_text.insert(tk.END, "2. Click 'Start Conversion' to upload and process\n")
        self.result_text.insert(tk.END, "3. Wait for processing to complete\n")
        self.result_text.insert(tk.END, "4. Results will appear here\n")

    def clear_log(self):
        self.result_text.delete(1.0, tk.END)
        self.show_welcome()

    def select_files(self):
        files = filedialog.askopenfilenames(title="Select PDF files", filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")])
        if files:
            self.selected_files = list(files)
            self.update_file_list()

    def clear_files(self):
        self.selected_files = []
        self.update_file_list()

    def update_file_list(self):
        self.file_listbox.delete(0, tk.END)
        for f in self.selected_files:
            name = Path(f).name
            size = Path(f).stat().st_size / (1024 * 1024)
            self.file_listbox.insert(tk.END, f"{name} ({size:.1f} MB)")

    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.result_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.result_text.see(tk.END)
        self.root.update()

    def set_status(self, message):
        self.status_var.set(message)
        self.root.update()

    def run_git(self, args):
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        result = subprocess.run(["git"] + args, cwd=self.config.repo_dir, capture_output=True, text=True, encoding="utf-8", errors="replace", startupinfo=startupinfo, creationflags=subprocess.CREATE_NO_WINDOW)
        return result.returncode == 0, result.stdout, result.stderr

    def start_conversion(self):
        if not self.selected_files:
            messagebox.showwarning("Warning", "Please select PDF files first")
            return
        if self.processing:
            messagebox.showinfo("Info", "Processing in progress. Please wait.")
            return
        self.processing = True
        self.btn_convert.config(state="disabled")
        self.progress.start(10)
        self.current_job_files = [Path(f).stem for f in self.selected_files]
        thread = threading.Thread(target=self.conversion_thread, daemon=True)
        thread.start()

    def conversion_thread(self):
        try:
            self.result_text.delete(1.0, tk.END)
            self.log("Starting conversion process...")
            self.log(f"Files: {', '.join(self.current_job_files)}")
            self.set_status("Updating repository...")
            self.run_git(["pull", "origin", "main"])
            self.set_status("Copying PDF files...")
            self.config.input_dir.mkdir(exist_ok=True)
            for pdf_path in self.selected_files:
                pdf = Path(pdf_path)
                dest = self.config.input_dir / pdf.name
                shutil.copy2(pdf, dest)
                self.log(f"Copied: {pdf.name}")
            self.set_status("Uploading to GitHub...")
            self.run_git(["add", "input/*.pdf"])
            file_names = ", ".join(Path(f).name for f in self.selected_files)
            self.run_git(["commit", "-m", f"Add PDFs: {file_names}"])
            success, out, err = self.run_git(["push", "origin", "main"])
            if success:
                self.log("Upload complete")
            else:
                self.log(f"Push warning: {err[:100]}")
            self.set_status("Processing on GitHub Actions...")
            self.log("Waiting for GitHub Actions...")
            self.wait_for_completion()
        except Exception as e:
            self.log(f"Error: {str(e)}")
            self.set_status("Error occurred")
        finally:
            self.processing = False
            self.progress.stop()
            self.root.after(0, lambda: self.btn_convert.config(state="normal"))

    def wait_for_completion(self):
        max_wait = 7200
        start_time = time.time()
        last_status = ""
        while time.time() - start_time < max_wait:
            self.run_git(["pull", "origin", "main"])
            status_file = self.config.logs_dir / "queue_status.json"
            if status_file.exists():
                try:
                    with open(status_file, "r", encoding="utf-8") as f:
                        status = json.load(f)
                    jobs = status.get("jobs", [])
                    current_jobs = [j for j in jobs if Path(j.get("filename", "")).stem in self.current_job_files]
                    if current_jobs:
                        completed = sum(1 for j in current_jobs if j.get("status") == "completed")
                        pending = sum(1 for j in current_jobs if j.get("status") in ("pending", "processing"))
                        failed = sum(1 for j in current_jobs if j.get("status") == "failed")
                        status_msg = f"Done: {completed} / Processing: {pending} / Failed: {failed}"
                        if status_msg != last_status:
                            self.log(status_msg)
                            last_status = status_msg
                        self.set_status(f"Processing... {status_msg}")
                        if pending == 0 and (completed > 0 or failed > 0):
                            self.log("All processing complete!")
                            self.download_results()
                            return
                except:
                    pass
            current_outputs = [o for o in self.config.output_dir.glob("*.md") if o.stem in self.current_job_files]
            if current_outputs:
                self.log(f"{len(current_outputs)} file(s) found")
                self.download_results()
                return
            time.sleep(self.config.check_interval)
        self.log("Timeout")
        self.set_status("Timeout")

    def download_results(self):
        self.set_status("Downloading results...")
        self.run_git(["pull", "origin", "main"])
        outputs = [o for o in self.config.output_dir.glob("*.md") if o.stem in self.current_job_files]
        if outputs:
            self.log("")
            self.log("=" * 50)
            self.log("CONVERSION RESULTS")
            self.log("=" * 50)
            for o in outputs:
                size = o.stat().st_size / 1024
                self.log(f"  {o.name} ({size:.1f} KB)")
            self.log("=" * 50)
            content = outputs[0].read_text(encoding="utf-8", errors="replace")
            preview = content[:3000]
            self.log(f"\nPreview of {outputs[0].name}:")
            self.log("-" * 50)
            self.result_text.insert(tk.END, preview + "\n")
            if len(content) > 3000:
                self.log(f"... ({len(content)} chars total)")
            self.result_text.see(tk.END)
        else:
            self.log("No output files found")
        self.set_status("Complete!")

    def open_output_folder(self):
        self.config.output_dir.mkdir(exist_ok=True)
        os.startfile(self.config.output_dir)


def main():
    root = tk.Tk()
    app = PDFConverterApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
