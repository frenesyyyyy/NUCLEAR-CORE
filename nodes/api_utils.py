import time
from rich.console import Console

console = Console()

def execute_with_backoff(api_call_func, max_retries=3, initial_delay=2.0, backoff_factor=2.0):
    """
    Executes an API call with exponential backoff.
    Removes the need for naive hardcoded sleeps, vastly speeding up queries while maintaining 429 safety.
    """
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return api_call_func()
        except Exception as e:
            # Catch 429 Too Many Requests to scale backoff gracefully
            is_rate_limit = "429" in str(e) or "quota" in str(e).lower()
            if attempt == max_retries - 1:
                console.print(f"[bold red]API Final Failure after {max_retries} attempts: {e}[/bold red]")
                raise e
            wait_time = delay * 2 if is_rate_limit else delay
            console.print(f"[yellow]API Error (Attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...[/yellow]")
            time.sleep(wait_time)
            delay *= backoff_factor
