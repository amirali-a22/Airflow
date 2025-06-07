from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Connect to MongoDB
client = MongoClient("mongodb://admin:admin@localhost:27017/")
db = client["linkedin"]
collection = db["linkedin_person"]

# List of major Iranian cities for location checking
IRANIAN_CITIES = [
    "tehran", "isfahan", "shiraz", "mashhad", "tabriz", "karaj", "ahvaz",
    "qom", "kermanshah", "urmia", "rasht", "zahedan", "hamadan", "yazd"
]

# Common Persian first names and surnames for nationality inference
PERSIAN_FIRST_NAMES = [
    "kasra", "pourya", "ali", "mohammad", "reza", "hassan", "mehdi", "sina",
    "farhad", "arash", "dariush", "nima", "babak", "parsa", "kaveh"
]
PERSIAN_SURNAMES = [
    "khakpour", "rahmati", "mohammadi", "hassani", "rezai", "ahmadi",
    "shariati", "ghasemi", "ebrahimi", "rahimi", "yazdani", "moradi"
]


# Helper function to extract string from potential dictionary
def get_string_field(data, keys):
    """
    Extract a string from a dictionary field, handling nested dictionaries.

    Args:
        data (dict): The dictionary to search (e.g., profile_info)
        keys (list): List of possible field names (e.g., ['location', 'current_city'])

    Returns:
        str: Extracted string or empty string if not found
    """
    for key in keys:
        value = data.get(key)
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            for subkey in ["city", "country", "name", "region"]:
                subvalue = value.get(subkey)
                if isinstance(subvalue, str):
                    return subvalue
    return ""


def check_iranian_and_location(username):
    """
    Check if a LinkedIn user is Iranian and living outside Iran based on MongoDB data.

    Args:
        username (str): The LinkedIn username to query (e.g., 'kasra-khakpour-a9881115b')

    Returns:
        dict: Result containing nationality and location findings
    """
    try:
        # Query the collection for the user
        user_data = collection.find_one({"username": username})

        if not user_data:
            return {
                "username": username,
                "status": "not_found",
                "message": "User not found in the database."
            }

        result = {
            "username": username,
            "is_likely_iranian": False,
            "lives_outside_iran": None,  # None if undetermined, True/False otherwise
            "details": [],
            "raw_data": {}  # For debugging
        }

        # Check username and profile_info for Persian names
        username_lower = username.lower()
        profile_info = user_data.get("profile_info", {})
        result["raw_data"]["profile_info"] = profile_info  # Debug: store raw data

        # Check username for Persian first names or surnames
        name_indicators = []
        if any(name in username_lower for name in PERSIAN_FIRST_NAMES):
            name_indicators.append("first name")
        if any(surname in username_lower for surname in PERSIAN_SURNAMES):
            name_indicators.append("surname")
        if name_indicators:
            result["is_likely_iranian"] = True
            result["details"].append(
                f"Username '{username}' suggests Iranian heritage (contains Persian {', '.join(name_indicators)})."
            )

        # Check profile_info first_name and last_name
        first_name = get_string_field(profile_info, ["first_name", "given_name"]) or ""
        last_name = get_string_field(profile_info, ["last_name", "family_name"]) or ""
        if first_name and any(name in first_name.lower() for name in PERSIAN_FIRST_NAMES):
            result["is_likely_iranian"] = True
            result["details"].append(f"First name '{first_name}' suggests Iranian heritage (Persian name).")
        if last_name and any(surname in last_name.lower() for surname in PERSIAN_SURNAMES):
            result["is_likely_iranian"] = True
            result["details"].append(f"Last name '{last_name}' suggests Iranian heritage (Persian surname).")

        # Check profile_info for location
        location = get_string_field(profile_info, ["location", "current_city", "country"])
        if location:
            location_lower = location.lower()
            if any(city in location_lower for city in IRANIAN_CITIES) or "iran" in location_lower:
                result["lives_outside_iran"] = False
                result["details"].append(f"Profile location '{location}' indicates residence in Iran.")
            else:
                result["lives_outside_iran"] = True
                result["details"].append(f"Profile location '{location}' indicates residence outside Iran.")

        # Check profile_info for bio
        bio = get_string_field(profile_info, ["bio", "about", "summary"])
        if bio and ("iranian" in bio.lower() or "persian" in bio.lower() or "farsi" in bio.lower()):
            result["is_likely_iranian"] = True
            result["details"].append(
                "Profile bio indicates Iranian nationality (mentions 'Iranian', 'Persian', or 'Farsi').")

        # Check experiences for recent job location
        experiences = user_data.get("experiences", [])
        result["raw_data"]["experiences"] = experiences  # Debug: store raw data
        if experiences:
            recent_experience = max(experiences,
                                    key=lambda x: x.get("start_date", "") if isinstance(x.get("start_date", ""),
                                                                                        str) else "", default={})
            job_location = get_string_field(recent_experience, ["location", "company_location"])
            if job_location:
                job_location_lower = job_location.lower()
                if any(city in job_location_lower for city in IRANIAN_CITIES) or "iran" in job_location_lower:
                    result["lives_outside_iran"] = False
                    result["details"].append(f"Recent job location '{job_location}' indicates residence in Iran.")
                else:
                    result["lives_outside_iran"] = True
                    result["details"].append(f"Recent job location '{job_location}' indicates residence outside Iran.")

        # Check educations for recent study location
        educations = user_data.get("educations", [])
        result["raw_data"]["educations"] = educations  # Debug: store raw data
        if educations:
            recent_education = max(educations, key=lambda x: x.get("end_date") or x.get("start_date", "") if isinstance(
                x.get("end_date") or x.get("start_date", ""), str) else "", default={})
            edu_location = get_string_field(recent_education, ["location", "institution_location"])
            if edu_location:
                edu_location_lower = edu_location.lower()
                if any(city in edu_location_lower for city in IRANIAN_CITIES) or "iran" in edu_location_lower:
                    result["lives_outside_iran"] = False
                    result["details"].append(f"Recent education location '{edu_location}' indicates residence in Iran.")
                else:
                    result["lives_outside_iran"] = True
                    result["details"].append(
                        f"Recent education location '{edu_location}' indicates residence outside Iran.")

        # Check skills for Persian/Farsi proficiency
        skills = user_data.get("skills", [])
        if any(isinstance(skill, str) and ("farsi" in skill.lower() or "persian" in skill.lower()) for skill in skills):
            result["is_likely_iranian"] = True
            result["details"].append("Skills include Farsi/Persian, supporting Iranian heritage.")

        # If location is still undetermined
        if result["lives_outside_iran"] is None:
            result["details"].append(
                "Unable to determine current location due to insufficient data (no valid location in profile, experiences, or educations).")

        return result

    except Exception as e:
        return {
            "username": username,
            "status": "error",
            "message": f"Error processing data: {str(e)}"
        }


def main():
    # Target username
    # username = "shirin-ataie-19007562"
    username = "abed-saeed-4b802819"
    # username = "kourosh-saadat-talab-255b56b7"
    # username = "kourosh-saadat-talab-255b56b7"
    # username = "aref-moltaji-hagh-12540b153"
    # username = "aida-koohkesh"
    # username = "kasra-khakpour-a9881115b"

    # Run analysis
    result = check_iranian_and_location(username)

    # Print results
    print(f"Analysis for username: {result['username']}")
    if result.get("status") in ["not_found", "error"]:
        print(result["message"])
    else:
        print(f"Is likely Iranian: {result['is_likely_iranian']}")
        print(f"Lives outside Iran: {result['lives_outside_iran']}")
        print("Details:")
        for detail in result["details"]:
            print(f"- {detail}")


if __name__ == "__main__":
    main()

# Close MongoDB connection
client.close()
