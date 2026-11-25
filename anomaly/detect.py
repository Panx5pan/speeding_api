import os

FIELDS = {
    "speed": "speed_mph",
    "violation": "speed_overlimit_mph",
}


def env_float(name, default):
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return float(value)


THRESHOLDS = {
    "speed": {
        "above": env_float("SPEED_ABOVE_MPH", 100.0),
        "below": env_float("SPEED_BELOW_MPH", 0.0),
    },
    "violation": {
        "above": env_float("VIOLATION_ABOVE_MPH", 40.0),
        "below": env_float("VIOLATION_BELOW_MPH", None),
    },
}


def check(event, above=None, below=None):
    if not isinstance(event, dict):
        return None
    field = FIELDS.get(event.get("type"))
    if field is None:
        return None
    value = event.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if above is not None and value > above:
        return f"{field} {value} is above {above}"
    if below is not None and value < below:
        return f"{field} {value} is below {below}"
    return None


def check_default(event):
    if not isinstance(event, dict):
        return None
    limits = THRESHOLDS.get(event.get("type"))
    if limits is None:
        return None
    return check(event, above=limits["above"], below=limits["below"])