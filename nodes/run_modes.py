RUN_MODES = {
    "lite": {
        "max_workers": 2,
        "parallel_enabled": False,
        "research_enabled": True,
        "analytics_enabled": True,
    },
    "standard": {
        "max_workers": 2,
        "parallel_enabled": False,
        "research_enabled": True,
        "analytics_enabled": True,
    },
    "agency": {
        "max_workers": 10,
        "parallel_enabled": True,
        "research_enabled": True,
        "analytics_enabled": True,
    },
}