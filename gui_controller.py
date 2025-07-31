import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import time
import json
import signal
import os
import sys

process = None
timer_running = False
start_time = None

PLACEMENTS = [
    'left_wrist', 'left_front_pocket', 'left_back_pocket', 'left_hand', 'left_arm',
    'right_wrist', 'right_front_pocket', 'right_back_pocket', 'right_hand', 'right_arm',
    'back',
]

ACTIVITIES = ['slow_walk', 'normal_walk', 'fast_walk', 'idle']

# ---------------- Helpers -----------------
def _terminate_process_tree(proc):
    try:
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            proc.terminate()
        else:
            proc.terminate()
        proc.wait(timeout=5)
    except Exception as e:
        print("Error stopping process:", e)

# ---------------- Callbacks -----------------
def start_recording():
    global process, timer_running, start_time

    if process is not None:
        stop_recording()

    subjects = []
    for i in range(3):
        sid = subject_id_entries[i].get().strip()
        activity = activity_vars[i].get().strip()
        placement = placement_vars[i].get().strip()
        if not sid.isdigit() or not activity or not placement:
            status_label.config(text=f"Fill all fields correctly for Subject {i+1}.")
            return
        subjects.append({
            "subject_id": int(sid),
            "activity": activity,
            "placement": placement
        })

    script_path = os.path.join(os.path.dirname(__file__), "multiple_sensorloggers_postgresql.py")
    cmd = [sys.executable, script_path, json.dumps(subjects)]

    try:
        process = subprocess.Popen(cmd)
    except FileNotFoundError:
        messagebox.showerror("Error", "multiple_sensorloggers_postgresql.py not found.")
        return

    start_time = time.time()
    timer_running = True
    update_timer()
    status_label.config(text="Recording started.")


def stop_recording():
    global process, timer_running

    if not process:
        status_label.config(text="Nothing to stop.")
        return

    timer_running = False
    timer_label.config(text="00:00:00")

    _terminate_process_tree(process)
    process = None
    status_label.config(text="Recording stopped.")


def update_timer():
    if not timer_running:
        return
    elapsed = int(time.time() - start_time)
    hrs = elapsed // 3600
    mins = (elapsed % 3600) // 60
    secs = elapsed % 60
    timer_label.config(text=f"{hrs:02d}:{mins:02d}:{secs:02d}")
    window.after(1000, update_timer)

# ---------------- GUI -----------------
window = tk.Tk()
window.title("Sensor Logger GUI - Multi-subject")
window.geometry("760x500")

label_font = ("Arial", 12)
entry_font = ("Arial", 12)

subject_id_entries = []
activity_vars = []
placement_vars = []

for i in range(3):
    frame = tk.LabelFrame(window, text=f"Subject Slot {i+1}", font=("Arial", 14, "bold"), padx=10, pady=10)
    frame.pack(fill="x", padx=10, pady=5)

    tk.Label(frame, text="Subject ID:", font=label_font).grid(row=0, column=0, sticky="w")
    sid_entry = tk.Entry(frame, font=entry_font, width=10)
    sid_entry.grid(row=0, column=1, sticky="w", padx=5)
    subject_id_entries.append(sid_entry)

    tk.Label(frame, text="Activity:", font=label_font).grid(row=0, column=2, sticky="w")
    activity_var = tk.StringVar()
    activity_dropdown = ttk.Combobox(frame, textvariable=activity_var, font=entry_font, width=18,
                                     values=ACTIVITIES, state="readonly")
    activity_dropdown.grid(row=0, column=3, sticky="w", padx=5)
    activity_vars.append(activity_var)

    tk.Label(frame, text="Phone Placement:", font=label_font).grid(row=0, column=4, sticky="w")
    placement_var = tk.StringVar()
    placement_dropdown = ttk.Combobox(frame, textvariable=placement_var, font=entry_font, width=18,
                                      values=PLACEMENTS, state="readonly")
    placement_dropdown.grid(row=0, column=5, sticky="w", padx=5)
    placement_vars.append(placement_var)

# Timer & Status
timer_label = tk.Label(window, text="00:00:00", font=("Arial", 24), fg="red")
timer_label.pack(pady=10)

status_label = tk.Label(window, text="", font=("Arial", 12), fg="red")
status_label.pack(pady=5)

# Buttons
button_frame = tk.Frame(window)
button_frame.pack(pady=15)

start_button = tk.Button(button_frame, text="Start", command=start_recording, bg='green', fg='black', font=("Arial", 14), width=12)
start_button.grid(row=0, column=0, padx=15)

stop_button = tk.Button(button_frame, text="Stop", command=stop_recording, bg='red', fg='black', font=("Arial", 14), width=12)
stop_button.grid(row=0, column=1, padx=15)

window.mainloop()
