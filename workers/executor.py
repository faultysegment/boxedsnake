import subprocess
import tempfile
import json
import os
import logging
from config import config

logger = logging.getLogger(__name__)

class TaskExecutionError(Exception):
    pass

def execute_task(task_id: str, script_content: str, env_vars: dict, timeout_seconds: int = None) -> dict:
    """
    Executes the given python script in a temporary directory via subprocess.
    Returns a dictionary containing the execution result, stdout, and stderr.
    """
    if timeout_seconds is None:
        timeout_seconds = config.DEFAULT_TIMEOUT_SECONDS

    with tempfile.TemporaryDirectory() as temp_dir:
        script_path = os.path.join(temp_dir, "script.py")
        output_path = os.path.join(temp_dir, "output.json")
        
        wrapper_code = """
# --- BOXED SNAKE WRAPPER ---
if __name__ == "__main__":
    import sys
    try:
        res = run()
        if res == "ok":
            send_result()
        else:
            print(f"Error: run() returned '{res}' instead of 'ok'. Skipping send_result().", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
"""
        full_script = script_content + "\n" + wrapper_code
        
        # Write the script content
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(full_script)

        # Prepare environment variables
        run_env = os.environ.copy()
        run_env.update(env_vars or {})
        run_env["BOXED_SNAKE_OUTPUT_FILE"] = output_path

        try:
            logger.info(f"Executing task {task_id} with timeout {timeout_seconds}s")
            process = subprocess.run(
                ["python3", script_path],
                env=run_env,
                cwd=temp_dir,
                capture_output=True,
                timeout=timeout_seconds,
                text=True
            )
            
            status = "success" if process.returncode == 0 else "failed"
            
            # Truncate large outputs if necessary
            stdout = process.stdout
            if len(stdout) > config.MAX_OUTPUT_SIZE_BYTES:
                stdout = stdout[:config.MAX_OUTPUT_SIZE_BYTES] + "\n...[TRUNCATED]"
                
            stderr = process.stderr
            if len(stderr) > config.MAX_OUTPUT_SIZE_BYTES:
                stderr = stderr[:config.MAX_OUTPUT_SIZE_BYTES] + "\n...[TRUNCATED]"

            result_data = None
            if os.path.exists(output_path):
                try:
                    with open(output_path, "r", encoding="utf-8") as f:
                        result_data = json.load(f)
                except json.JSONDecodeError as e:
                    logger.warning(f"Task {task_id} output file contains invalid JSON: {e}")
                    stderr += f"\nError parsing output JSON: {e}"

            return {
                "task_id": task_id,
                "status": status,
                "result_data": result_data,
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": process.returncode
            }

        except subprocess.TimeoutExpired as e:
            logger.error(f"Task {task_id} timed out after {timeout_seconds}s")
            
            stdout = e.stdout.decode('utf-8', errors='replace') if e.stdout else ""
            stderr = e.stderr.decode('utf-8', errors='replace') if e.stderr else ""
            
            return {
                "task_id": task_id,
                "status": "timeout",
                "result_data": None,
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": -1
            }
        except Exception as e:
            logger.exception(f"Task {task_id} failed with unexpected error")
            return {
                "task_id": task_id,
                "status": "error",
                "result_data": None,
                "stdout": "",
                "stderr": str(e),
                "exit_code": -1
            }
