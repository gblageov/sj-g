import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext
from io import StringIO
import threading
from datetime import datetime

# Ensure we can import the local 'processing' package when running directly
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from processing.pipeline import process_woocommerce_to_shopify

class ConsoleOutput(StringIO):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
    
    def write(self, text):
        self.text_widget.config(state=tk.NORMAL)
        self.text_widget.insert(tk.END, text)
        self.text_widget.see(tk.END)
        self.text_widget.update_idletasks()
        self.text_widget.config(state=tk.DISABLED)

class ShopifyConverterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("WooCommerce to Shopify Converter")
        self.root.geometry("800x600")
        
        # Configure grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)
        
        # Top frame for controls
        self.top_frame = ttk.Frame(root, padding="10")
        self.top_frame.grid(row=0, column=0, sticky="ew")
        
        # File selection
        self.file_path = tk.StringVar()
        ttk.Label(self.top_frame, text="Input File:").grid(row=0, column=0, sticky="w")
        ttk.Entry(self.top_frame, textvariable=self.file_path, width=50).grid(row=0, column=1, padx=5, sticky="ew")
        ttk.Button(self.top_frame, text="Browse...", command=self.browse_file).grid(row=0, column=2, padx=5)
        
        # Run button
        self.run_button = ttk.Button(self.top_frame, text="Run Conversion", command=self.start_conversion)
        self.run_button.grid(row=0, column=3, padx=5)
        
        # Console output
        self.console_frame = ttk.LabelFrame(root, text="Console Output", padding="10")
        self.console_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        self.console_frame.columnconfigure(0, weight=1)
        self.console_frame.rowconfigure(0, weight=1)
        
        self.console = scrolledtext.ScrolledText(
            self.console_frame, 
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=('Consolas', 10)
        )
        self.console.grid(row=0, column=0, sticky="nsew")
        
        # Redirect stdout and stderr to our console
        sys.stdout = ConsoleOutput(self.console)
        sys.stderr = ConsoleOutput(self.console)
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.status_bar = ttk.Label(root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.grid(row=2, column=0, sticky="ew")
        
        # Configure grid weights
        self.top_frame.columnconfigure(1, weight=1)
        
    def browse_file(self):
        filename = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=(("Excel files", "*.xlsx"), ("All files", "*.*"))
        )
        if filename:
            self.file_path.set(filename)
    
    def start_conversion(self):
        input_file = self.file_path.get()
        
        if not input_file:
            self.status_var.set("Error: Please select an input file")
            return
            
        if not os.path.exists(input_file):
            self.status_var.set(f"Error: File not found: {input_file}")
            return
            
        # Disable run button during processing
        self.run_button.config(state=tk.DISABLED)
        self.status_var.set("Processing...")
        
        # Run in a separate thread to keep the UI responsive
        thread = threading.Thread(target=self.run_conversion, args=(input_file,))
        thread.daemon = True
        thread.start()
    
    def run_conversion(self, input_file):
        try:
            # Get the output path (same as input but with _output and timestamp)
            base_name = os.path.splitext(input_file)[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_excel = f"{base_name}_output_{timestamp}.xlsx"
            output_txt = f"{base_name}_output_{timestamp}.txt"
            
            # Redirect console output to both the GUI and a file
            class TeeOutput:
                def __init__(self, *files):
                    self.files = files
                def write(self, obj):
                    for f in self.files:
                        f.write(obj)
                def flush(self):
                    for f in self.files:
                        if hasattr(f, 'flush'):
                            f.flush()
            
            with open(output_txt, 'w', encoding='utf-8') as f:
                # Save original stdout
                original_stdout = sys.stdout
                original_stderr = sys.stderr
                
                try:
                    # Redirect both stdout and stderr to both console and file
                    sys.stdout = TeeOutput(original_stdout, f)
                    sys.stderr = TeeOutput(original_stderr, f)
                    
                    # Run the conversion
                    print(f"Starting conversion of: {input_file}")
                    print("-" * 50)
                    
                    # Call your existing processing function
                    output = process_woocommerce_to_shopify(input_file, output_file=output_excel)
                    
                    if output:
                        print("\n" + "=" * 50)
                        print(f"Conversion completed successfully!")
                        print(f"Output Excel: {output_excel}")
                        print(f"Log file: {output_txt}")
                        self.status_var.set("Conversion completed successfully!")
                    else:
                        self.status_var.set("Error during conversion. Check console for details.")
                        
                except Exception as e:
                    print(f"\nError: {str(e)}", file=sys.stderr)
                    self.status_var.set(f"Error: {str(e)}")
                    
                finally:
                    # Restore original stdout/stderr
                    sys.stdout = original_stdout
                    sys.stderr = original_stderr
                    
        except Exception as e:
            print(f"Unexpected error: {str(e)}", file=sys.stderr)
            self.status_var.set(f"Unexpected error: {str(e)}")
            
        finally:
            # Re-enable the run button
            self.root.after(0, lambda: self.run_button.config(state=tk.NORMAL))

def main():
    root = tk.Tk()
    app = ShopifyConverterApp(root)
    
    # Set application icon and style
    try:
        root.iconbitmap("icon.ico")  # Optional: add an icon file
    except:
        pass  # Icon not found, use default
    
    # Set a modern theme if available
    try:
        from tkinter import ttk
        style = ttk.Style()
        style.theme_use('clam')  # Try different themes: 'clam', 'alt', 'default', 'classic'
    except:
        pass
    
    # Start the application
    root.mainloop()

if __name__ == "__main__":
    main()
