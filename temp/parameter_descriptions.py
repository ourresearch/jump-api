scenario_configs = {
    "cost_alacart_increase": {
        "name": "cost_alacart_increase",
        "default": 8,
        "value": None,
        "display": "percent",
        "display_name": "À la carte subscription cost growth",
        "descr": "The annual percent increase you expect in à la carte subscription prices (literature suggests 8%).",
    },
    "cost_bigdeal": {
        "name": "cost_bigdeal",
        "default": 2100000,
        "value": None,
        "display": "dollars",
        "display_name": "Big Deal cost",
        "descr": "The cost of your Big Deal right now (or of the bundle of à la carte subscriptions, if you don't have a Big Deal)."
    },
    "cost_bigdeal_increase": {
        "name": "cost_bigdeal_increase",
        "default": 5,
        "value": None,
        "display": "percent",
        "display_name": "Big Deal growth",
        "descr": "The annual percent increase in your Big Deal price (literature suggests average is 5% if a Big Deal, 8% if individual subscriptions)."

    },
    "cost_content_fee_percent": {
        "name": "cost_content_fee_percent",
        "default": 5.7,
        "value": None,
        "display": "percent",
        "display_name": "À la carte 'content fee'",
        "descr": "A content fee charged by publishers when buying subscriptions à la carte, above whatever is included in your current package price (literature suggests 5.7% for subscriptions)."
    },
    "cost_ill": {
        "name": "cost_ill",
        "default": 17,
        "value": None,
        "display": "dollars",
        "display_name": "ILL transaction cost",
        "descr": "The cost of an ILL request for your institution (literature suggests $17 is average).",
    },
    "ill_request_percent_of_delayed": {
        "name": "ill_request_percent_of_delayed",
        "default": 5,
        "value": None,
        "display": "percent",
        "display_name": "ILL frequency, as percent of delayed access",
        "descr": "The percent of accesses which you estimate will result in ILL requests, of papers not available instantly (literature suggests 5).",
    },
    "include_bronze": {
        "name": "include_bronze",
        "default": True,
        "value": None,
        "display": "boolean",
        "display_name": "Include Bronze OA",
        "descr": "Include Bronze OA as a type of fulfillment.  Bronze OA is when a paper is made freely available on a publisher site without an open license (includes Elsevier's \"open archive\" journals).",
    },
    "include_submitted_version": {
        "name": "include_submitted_version",
        "default": True,
        "value": None,
        "display": "boolean",
        "display_name": "Permit non-peer-reviewed versions",
        "descr": "For Green OA, allow submitted versions as a type of fulfillment.  Submitted versions are papers made available in repositories as preprints or other versions that have not yet been peer reviewed.",
    },
    "include_social_networks": {
        "name": "include_social_networks",
        "default": True,
        "value": None,
        "display": "boolean",
        "display_name": "Include ResearchGate-hosted content",
        "descr": "Include ResearchGate and other Academic Social Networks as a fulfillment source.",
    },
    "include_backfile": {
        "name": "include_backfile",
        "default": True,
        "value": None,
        "display": "boolean",
        "display_name": "Include perpetual-access backfile content",
        "descr": "Include backfile content as a type of fulfillment.  Disable to see what fulfillment would be like if you don't have perpetual access.",
    },
    "backfile_contribution": {
        "name": "backfile_contribution",
        "default": 100,
        "value": None,
        "display": "percent",
        "display_name": "Backfile available as perpetual access",
        "descr": """Percent of backfile available as perpetual access.  If you estimate that you have perpetual access to 90% of your content, set this to 90%.
        // We are discontinuing this parameter. Most users aren't using it. BUT there might be some users who actually
        // are using this now in production, and for them it will be super weird if it just disappears. So we will
        // hide it for everyone UNLESS you have set it to something other than the default."""
    },
    "weight_authorship": {
        "name": "weight_authorship",
        "default": 100,
        "value": None,
        "display": "number",
        "display_name": "Institutional authorship weight",
        "descr": "A paper authored by someone in your institution contributes this many download-equivalent points to the Usage of the journal.  A common value is 100 -- in this case an authored paper in this journal is modelled as the equivalent of 100 downloads of this journal.",
    },
    "weight_citation": {
        "name": "weight_citation",
        "default": 10,
        "value": None,
        "display": "number",
        "display_name": "Institutional citation weight",
        "descr": "A citation from someone in your institution contributes this many download-equivalent points to the Usage of the journal.  A common value is 10 -- in this case a citation from your institution to this journal is modelled as the equivalent of 10 downloads of this journal.",
    },
}

print("\n\n".join(["{}\n{}\nDefault: {} ({})".format(config["display_name"], config["descr"], config["default"], config["display"]) for config in scenario_configs.values()]))
print("\n".join(["{}: {}".format(config["display_name"], config["default"]) for config in scenario_configs.values()]))
