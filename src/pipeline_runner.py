"""
This is the main entrypoint that runs all pipeline steps.
"""
# ### IMPORTS ###
import logging
import importlib

# --- Configuration for DAILY jobs ---
DAILY_STEPS = [
    {"module": "src.extract_openaq_locations", "function": "main"},
    {"module": "src.extract_openaq_sensors", "function": "main"},
    {"module": "src.extract_openaq_meausurements", "function": "main"},
    {"module": "src.transform_dim_location", "function": "main"}, 
    {"module": "src.transform_dim_sensor", "function": "main"}, 
    {"module": "src.transform_fact_measurements", "function": "main"}, 
    {"module": "src.load_dim_location", "function": "load_dim_location"},
    {"module": "src.load_dim_sensor", "function": "load_dim_sensor"},
    {"module": "src.load_fact_measurements", "function": "load_fact_measurements"},
]

# --- Configuration for WEEKLY jobs ---
WEEKLY_STEPS = [
    {"module": "src.extract_openaq_parameters", "function": "main"}, 
    {"module": "src.transform_dim_parameter", "function": "main"}, 
    {"module": "src.load_dim_parameter", "function": "load_dim_parameter"},
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

def _execute_steps(steps_config: list):
    """A generic helper function to run a list of pipeline steps."""
    logging.info(f"Runner starting. Executing {len(steps_config)} steps.")
    for i, step in enumerate(steps_config):
        module_name = step["module"]
        function_name = step["function"]
        logging.info(f"--- Running Step {i+1}/{len(steps_config)}: Calling {module_name}.{function_name} ---")
        try:
            module = importlib.import_module(module_name)
            task_function = getattr(module, function_name)
            task_function()
            logging.info(f"--- Step {i+1} finished successfully. ---")
        except Exception as e:
            logging.error(f"!!! Step {i+1} ({module_name}.{function_name}) failed: {e} !!!")
            # This 'raise' is crucial. It stops the function and tells Airflow the task failed.
            raise
    logging.info("Runner finished successfully.")

def run_daily_pipeline():
    """Entrypoint for the daily pipeline."""
    _execute_steps(DAILY_STEPS)

def run_weekly_pipeline():
    """Entrypoint for the weekly pipeline."""
    _execute_steps(WEEKLY_STEPS)

# =============================================================================
# SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    import sys

    # Check if a command-line argument was provided (e.g., 'daily' or 'weekly')
    if len(sys.argv) > 1:
        pipeline_to_run = sys.argv[1]

        if pipeline_to_run == "daily":
            print("--- Manually starting DAILY pipeline runner ---")
            run_daily_pipeline()
        elif pipeline_to_run == "weekly":
            print("--- Manually starting WEEKLY pipeline runner ---")
            run_weekly_pipeline()
        else:
            print(f"Error: Unknown argument '{pipeline_to_run}'. Please use 'daily' or 'weekly'.")
    else:
        print("Error: Please provide an argument. Use 'daily' or 'weekly'.")