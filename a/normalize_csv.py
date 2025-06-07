import csv
import json


def get_nested_value(data_dict, keys, default=''):
    """Safely get a nested value from a dictionary or list."""
    current = data_dict
    for key in keys:
        if isinstance(current, dict) and key in current and current[key] is not None:
            current = current[key]
        elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current) and current[
            key] is not None:
            current = current[key]
        else:
            return default
    return current if current is not None else default


def generate_csv_from_json(json_file_path, csv_file_path, num_experiences=3, num_educations=3):
    """
    Converts a JSON file containing a list of person profiles to a CSV file.

    Args:
        json_file_path (str): Path to the input JSON file.
        csv_file_path (str): Path to the output CSV file.
        num_experiences (int): Number of experiences to flatten per person.
        num_educations (int): Number of educations to flatten per person.
    Returns:
        str: A message indicating success or failure.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        return f"Error: The file {json_file_path} was not found."
    except json.JSONDecodeError:
        return f"Error: The file {json_file_path} is not a valid JSON file."
    except Exception as e:
        return f"An error occurred while reading the JSON file: {e}"

    if not isinstance(data, list):
        return "Error: JSON data is not a list of records. Expecting a JSON array at the top level."

    # Define base headers
    headers = [
        'username', 'first_name', 'last_name', 'full_name', 'headline',
        'public_identifier', 'profile_picture',
        'country_code', 'postal_code', 'full_location', 'city', 'state', 'country',
        'follower_count', 'connection_count',
        'overall_skills'  # Comma-separated list of top-level skills
    ]

    # Add experience headers
    for i in range(num_experiences):
        headers.extend([
            f'experience_{i + 1}_company_name', f'experience_{i + 1}_company_url', f'experience_{i + 1}_job_title',
            f'experience_{i + 1}_employment_type', f'experience_{i + 1}_start_date', f'experience_{i + 1}_end_date',
            f'experience_{i + 1}_duration_cooperation', f'experience_{i + 1}_skills',
            f'experience_{i + 1}_job_description',
            f'experience_{i + 1}_location', f'experience_{i + 1}_work_type'
        ])

    # Add education headers
    for i in range(num_educations):
        headers.extend([
            f'education_{i + 1}_school_name', f'education_{i + 1}_school_url', f'education_{i + 1}_degree',
            f'education_{i + 1}_major', f'education_{i + 1}_start_date', f'education_{i + 1}_end_date',
            f'education_{i + 1}_duration_study'
        ])

    # Add summary headers from experience_info and education_info
    headers.extend([
        'total_duration_of_cooperation', 'last_job_title', 'last_company_name',
        'companies_count',
        'latest_school_name', 'latest_degree', 'latest_major',
        'earliest_education_start_year', 'latest_education_end_year'
    ])

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers,
                                extrasaction='ignore')  # 'ignore' will prevent errors if a row has extra keys
        writer.writeheader()

        for record_index, record in enumerate(data):
            if not isinstance(record, dict):
                print(f"Warning: Skipping record at index {record_index} as it is not a dictionary.")
                continue

            row = {header: '' for header in headers}  # Initialize with empty strings to handle missing data gracefully
            # Basic profile info
            row['username'] = get_nested_value(record, ['username'])
            profile_info = get_nested_value(record, ['profile_info'], default={})
            row['first_name'] = get_nested_value(profile_info, ['first_name'])
            row['last_name'] = get_nested_value(profile_info, ['last_name'])
            row['full_name'] = get_nested_value(profile_info, ['full_name'])
            row['headline'] = get_nested_value(profile_info, ['headline'])
            row['public_identifier'] = get_nested_value(profile_info, ['public_identifier'])
            row['profile_picture'] = get_nested_value(profile_info, ['profile_picture'])

            location_info = get_nested_value(profile_info, ['location'], default={})
            row['country_code'] = get_nested_value(location_info, ['country_code'])
            row['postal_code'] = get_nested_value(location_info, ['postal_code'])
            row['full_location'] = get_nested_value(location_info, ['full_location'])
            row['city'] = get_nested_value(location_info, ['city'])
            row['state'] = get_nested_value(location_info, ['state'])
            row['country'] = get_nested_value(location_info, ['country'])

            row['follower_count'] = get_nested_value(profile_info, ['follower_count'])
            row['connection_count'] = get_nested_value(profile_info, ['connection_count'])

            skills_list = get_nested_value(record, ['skills'], default=[])
            if isinstance(skills_list, list):
                row['overall_skills'] = ', '.join(
                    str(s) for s in skills_list if s is not None)  # Ensure skills are strings

            # Experiences
            experiences = get_nested_value(record, ['experiences'], default=[])
            if isinstance(experiences, list):
                for i in range(min(len(experiences), num_experiences)):
                    exp = experiences[i]
                    if not isinstance(exp, dict): continue

                    row[f'experience_{i + 1}_company_name'] = get_nested_value(exp, ['company_name'])
                    row[f'experience_{i + 1}_company_url'] = get_nested_value(exp, ['company_url'])
                    row[f'experience_{i + 1}_job_title'] = get_nested_value(exp, ['job_title'])
                    row[f'experience_{i + 1}_employment_type'] = get_nested_value(exp, ['employment_type'])
                    row[f'experience_{i + 1}_start_date'] = get_nested_value(exp, ['start_date', 'date'])
                    row[f'experience_{i + 1}_end_date'] = get_nested_value(exp, ['end_date', 'date'])
                    row[f'experience_{i + 1}_duration_cooperation'] = get_nested_value(exp, ['duration_of_cooperation'])

                    exp_skills = get_nested_value(exp, ['skills'], default=[])
                    if isinstance(exp_skills, list):
                        row[f'experience_{i + 1}_skills'] = ', '.join(str(s) for s in exp_skills if s is not None)

                    row[f'experience_{i + 1}_job_description'] = get_nested_value(exp, ['job_description'])
                    row[f'experience_{i + 1}_location'] = get_nested_value(exp, ['location'])
                    row[f'experience_{i + 1}_work_type'] = get_nested_value(exp, ['work_type'])

            # Educations
            educations_list = get_nested_value(record, ['educations'], default=[])
            if isinstance(educations_list, list):
                for i in range(min(len(educations_list), num_educations)):
                    edu = educations_list[i]
                    if not isinstance(edu, dict): continue

    row[f'education_{i + 1}_school_name'] = get_nested_value(edu, ['school_name'])
    row[f'education_{i + 1}_school_url'] = get_nested_value(edu, ['school_url'])
    row[f'education_{i + 1}_degree'] = get_nested_value(edu, ['degree'])
    row[f'education_{i + 1}_major'] = get_nested_value(edu, ['major'])
    row[f'education_{i + 1}_start_date'] = get_nested_value(edu, ['start_date', 'date'])
    row[f'education_{i + 1}_end_date'] = get_nested_value(edu, ['end_date', 'date'])
    row[f'education_{i + 1}_duration_study'] = get_nested_value(edu, ['duration_of_study'])

    # Summary Info

    experience_info_summary = get_nested_value(record, ['experience_info'], default={})
    row['total_duration_of_cooperation'] = get_nested_value(experience_info_summary, ['total_duration_of_cooperation'])
    row['last_job_title'] = get_nested_value(experience_info_summary, ['last_job_title'])
    row['last_company_name'] = get_nested_value(experience_info_summary, ['last_company_name'])
    row['companies_count'] = get_nested_value(experience_info_summary, ['companies_count'])

    education_info_summary = get_nested_value(record, ['education_info'], default={})
    row['latest_school_name'] = get_nested_value(education_info_summary, ['latest_school_name'])
    row['latest_degree'] = get_nested_value(education_info_summary, ['latest_degree'])
    row['latest_major'] = get_nested_value(education_info_summary, ['latest_major'])
    row['earliest_education_start_year'] = get_nested_value(education_info_summary,
                                                            ['earliest_education_start_date', 'year'])
    row['latest_education_end_year'] = get_nested_value(education_info_summary, ['latest_education_end_date', 'year'])

    writer.writerow(row)

    return f"CSV file '{csv_file_path}' has been generated successfully."

    # --- How to use the script: ---
    # 1. Save the code above as a Python file (e.g., json_to_csv_converter.py).
    # 2. Place your person_temp.json file in the same directory as the script,
    #    or update the json_input_path variable in the script with the correct path.
    # 3. You can change the output CSV filename by modifying csv_output_path.
    # 4. You can adjust the number of experiences and education records to extract by changing
    #    the num_experiences_to_extract and num_educations_to_extract variables.
    # 5. Run the script from your terminal: python json_to_csv_converter.py
    # 6. A CSV file (e.g., person_output.csv) will be created in the specified location.


if __name__ == "__main__":
    json_input_path = "person_temp.json"  # Make sure this file is accessible
    csv_output_path = "person_output.csv"

    # You can customize the number of experiences and educations to extract
    num_experiences_to_extract = 3
    num_educations_to_extract = 3

    result_message = generate_csv_from_json(
        json_input_path,
        csv_output_path,
        num_experiences=num_experiences_to_extract,
        num_educations=num_educations_to_extract
    )
    print(result_message)
