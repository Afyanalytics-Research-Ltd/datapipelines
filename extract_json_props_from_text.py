import time
import os
import uuid
import json
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime
from openai import OpenAI
from openai import OpenAI, RateLimitError, APIConnectionError, APIStatusError, APIError




# ------------------- OpenAI Client -------------------
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
max_retries = 3
retry_delay = 2  # seconds

# ------------------- Logging -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ------------------- Paths -------------------
text_folder = Path("/opt/softwares/automations_and_data_pipelines/data/text_outputs/")  # folder containing text files
json_schema_file = Path("external_labresult_schema.json")  # external JSON schema file
csv_output = Path(f"external_lab_results_{uuid.uuid4().hex}.csv")

# Load JSON schema from file
with open(json_schema_file, "r", encoding="utf-8") as f:
    json_schema = json.load(f)

# ------------------- LLM Extraction Function -------------------
def extract_lab_result_from_text(text: str,retry_delay=2,max_retries=3) -> dict:
    """
    Uses GPT-4o-mini to extract LabResult JSON from text text.
    Returns a dictionary matching the JSON schema loaded from file.
    """
    system_prompt = (
        "You are an assistant that extracts structured lab result data "
        "from text medical reports. Return only valid JSON matching the following schema:\n"
        f"{json.dumps(json_schema, indent=2)}"
    )


    for attempt in range(1, max_retries + 1):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Extract lab result from the following text:\n\n{text}"}
                ],
                response_format={"type": "json_object"},
                temperature=0
            )

            # If no exception, break out of retry loop
            break

        except RateLimitError as e:
            print(f"Rate limit hit (429). Attempt {attempt}/{max_retries}. Retrying in {retry_delay} sec...")
            time.sleep(retry_delay)
            retry_delay *= 2  # exponential backoff
        except APIConnectionError as e:
            # network/connectivity issue
            raise Exception(f"API connection error: {e}")
        except APIStatusError as e:
            # non-200-range status code (4xx, 5xx)
            raise Exception(f"API status error: {e}")
        except APIError as e:
            # fallback for all other API‑related errors
            raise Exception(f"OpenAI API error: {e}")
        except Exception as e:
            # Catch anything else
            raise Exception(f"Unexpected error: {e}")

    try:
        output_text = response.choices[0].message.content
        return json.loads(output_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON from LLM output: {e}\nOutput: {output_text}")

# ------------------- Process text Files -------------------
text_files = list(text_folder.glob("*.txt"))
logging.info(f"Found {len(text_files)} text files to process.")

# Flag to write header only once
write_header = True

for text_file in text_files:
    try:
        logging.info(f"Processing file: {text_file.name}")
        text = text_file.read_text(encoding="utf-8")

        # Extract JSON via GPT-4o-mini
        json_record = extract_lab_result_from_text(text)

        # Create DataFrame for current row
        df_row = pd.DataFrame([{
            "patient_name": json_record.get("patient_name", ""),
            "json_record": json.dumps(json_record, ensure_ascii=False)
        }])

        # Append to CSV incrementally
        df_row.to_csv(csv_output, mode="a", index=False, header=write_header, encoding="utf-8")
        write_header = False  # after first write, don't write header again

        logging.info(f"Successfully processed {text_file.name}")
        file_path_to_remove = text_folder/text_file.name
        if os.path.exists(file_path_to_remove):
            try:
                os.remove(file_path_to_remove)
                print(f"🗑️  Deleted local file: {text_file.name}")
            except Exception as e:
                print(f"Error deleting local {text_file.name}: {e}")
        else:
            print(f"⚠️ Local file not found for deletion: {text_file.name}")

    except Exception as e:
        logging.error(f"Error processing {text_file.name}: {e}", exc_info=True)

logging.info(f"All files processed. Results saved to {csv_output}")
