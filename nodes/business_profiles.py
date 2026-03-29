"""Business intelligence layer for GEO specialization."""

BUSINESS_INTELLIGENCE_PROFILES = {
    "b2b_saas": {
        "label": "B2B SaaS",
        "macro_industry": "Software",
        "geo_behavior": "authority_driven",
        "query_style": "technical_authority",
        "scale_default": "Global",
        "location_enforce": False,
        "stress_test_budget": {"blind": 14, "contextual": 12, "branded": 8},
        "serper_mode": "global_leaders",
        "allowed_schema_types": ["Organization", "SoftwareApplication", "WebSite", "FAQPage", "BreadcrumbList"],
        "allowed_blueprint_signals": ["technical_authority", "use_case_expansion", "comparison_strategy"],
        "must_have_signals": [
            "Organization schema",
            "Product or SoftwareApplication schema",
            "use-case pages",
            "comparison pages",
            "integration pages",
            "pricing page"
        ],
        "risk_factors": [
            "generic homepage copy",
            "no use-case specificity",
            "weak branded demand",
            "no authority references"
        ],
        "persona_templates": [
            {
                "persona": "Founder / Operator",
                "intent": "problem-solving",
                "query_types": ["blind", "contextual"],
                "example_queries": [
                    "best tools for automating customer onboarding",
                    "best B2B software for reducing churn"
                ]
            },
            {
                "persona": "Head of Ops / Buyer",
                "intent": "comparison",
                "query_types": ["contextual", "branded"],
                "example_queries": [
                    "best customer support platforms for SaaS",
                    "enterprise software alternatives for support"
                ]
            },
            {
                "persona": "End User / Evaluator",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[brand] pricing for enterprises",
                    "[brand] reviews for SaaS"
                ]
            }
        ],
        "blind_fallback_templates": {
            "it": ["migliori software b2b", "piattaforme saas per aziende", "strumenti automazione business", "software gestione clienti b2b", "migliori tool produttività aziendale"],
            "en": ["best b2b software", "saas platforms for enterprise", "business automation tools", "customer management software b2b", "best enterprise productivity tools"]
        },
        "contextual_fallback_templates": {
            "it": ["come scegliere un software b2b", "migliori integrazioni saas per ufficio", "roi software b2b testimonianze"],
            "en": ["how to choose b2b software", "best saas integrations for office", "b2b software roi case studies"]
        }
    },

    "consumer_saas": {
        "label": "Consumer SaaS / App",
        "macro_industry": "Software",
        "geo_behavior": "discovery_plus_validation",
        "query_style": "feature_benefit",
        "scale_default": "Global",
        "location_enforce": False,
        "stress_test_budget": {"blind": 10, "contextual": 10, "branded": 8},
        "serper_mode": "global_leaders",
        "allowed_schema_types": ["SoftwareApplication", "WebSite", "FAQPage", "AggregateRating"],
        "allowed_blueprint_signals": ["feature_highlight", "trust_signals", "conversion_optimization"],
        "must_have_signals": [
            "app store presence",
            "reviews/testimonials",
            "feature pages",
            "FAQ",
            "pricing"
        ],
        "risk_factors": [
            "unclear differentiation",
            "weak trust signals",
            "no comparisons"
        ],
        "persona_templates": [
            {
                "persona": "Curious User",
                "intent": "discovery",
                "query_types": ["blind"],
                "example_queries": [
                    "best productivity apps",
                    "apps to organize my life"
                ]
            },
            {
                "persona": "Comparer",
                "intent": "decision",
                "query_types": ["contextual"],
                "example_queries": [
                    "notion vs evernote",
                    "best habit tracking app"
                ]
            },
            {
                "persona": "Buyer",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[brand] worth it",
                    "[brand] premium review"
                ]
            }
        ],
    },

    "ecommerce_brand": {
        "label": "E-commerce Brand",
        "macro_industry": "Retail",
        "geo_behavior": "comparison_driven",
        "query_style": "commercial_comparison",
        "scale_default": "National",
        "location_enforce": False,
        "stress_test_budget": {"blind": 8, "contextual": 12, "branded": 8},
        "serper_mode": "category_leaders",
        "allowed_schema_types": ["Product", "Offer", "AggregateOffer", "Review", "Organization"],
        "allowed_blueprint_signals": ["conversion_hooks", "social_proof", "category_authority"],
        "must_have_signals": [
            "Product schema",
            "Offer or AggregateOffer schema",
            "reviews",
            "collection/category pages",
            "shipping/returns info"
        ],
        "risk_factors": [
            "thin product descriptions",
            "no review proof",
            "no category authority content"
        ],
        "persona_templates": [
            {
                "persona": "Explorer",
                "intent": "discovery",
                "query_types": ["blind"],
                "example_queries": [
                    "best minimalist sneakers",
                    "best protein snacks online"
                ]
            },
            {
                "persona": "Comparer",
                "intent": "comparison",
                "query_types": ["contextual"],
                "example_queries": [
                    "nike vs adidas running shoes",
                    "best budget streetwear brands"
                ]
            },
            {
                "persona": "Buyer",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[brand] reviews",
                    "[product] worth it"
                ]
            }
        ],
    },

    "marketplace": {
        "label": "Marketplace / Platform",
        "macro_industry": "Platform",
        "geo_behavior": "authority_plus_category",
        "query_style": "market_matching",
        "scale_default": "National",
        "location_enforce": False,
        "stress_test_budget": {"blind": 12, "contextual": 10, "branded": 8},
        "serper_mode": "industry_leaders",
        "allowed_schema_types": ["Organization", "WebSite", "FAQPage", "BreadcrumbList", "SearchAction"],
        "allowed_blueprint_signals": ["platform_trust", "category_discovery", "partner_onboarding"],
        "must_have_signals": [
            "category landing pages",
            "trust and safety pages",
            "seller/buyer explainer pages",
            "FAQ"
        ],
        "risk_factors": [
            "two-sided value prop unclear",
            "weak category pages",
            "brand confusion"
        ],
        "persona_templates": [
            {
                "persona": "Supply Side",
                "intent": "opportunity seeking",
                "query_types": ["blind", "contextual"],
                "example_queries": [
                    "come iscrivere ristorante app delivery",
                    "diventare partner piattaforma consegne"
                ]
            },
            {
                "persona": "Demand Side",
                "intent": "discovery",
                "query_types": ["blind", "contextual"],
                "example_queries": [
                    "migliori consegne a domicilio",
                    "app per ordinare cibo",
                    "servizi delivery recensioni",
                    "ordinare spesa online veloce"
                ]
            },
            {
                "persona": "Evaluator",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[brand] legit",
                    "[brand] costi consegna"
                ]
            }
        ],
        "blind_fallback_templates": {
            "it": [
                "migliori app consegna cibo italia",
                "piattaforme delivery più usate in italia",
                "app per ordinare cibo a domicilio",
                "servizi delivery affidabili in italia",
                "alternative a uber eats italia",
                "app con più ristoranti convenzionati"
            ],
            "en": [
                "best food delivery apps",
                "most used delivery platforms",
                "apps for ordering food at home",
                "reliable delivery services",
                "alternatives to main delivery apps",
                "apps with most partner restaurants"
            ]
        },
        "contextual_fallback_templates": {
            "it": [
                "come funzionano le app di delivery",
                "quanto costa il delivery in italia",
                "consegna spesa a domicilio opinioni",
                "migliori servizi per ordinare cena online"
            ],
            "en": [
                "how do delivery apps work",
                "cost of food delivery services",
                "grocery delivery reviews",
                "best services to order dinner online"
            ]
        }
    },

    "local_dentist": {
        "label": "Local Dentist",
        "macro_industry": "Healthcare",
        "geo_behavior": "proximity_trust",
        "query_style": "medical_trust",
        "scale_default": "Local",
        "location_enforce": True,
        "stress_test_budget": {"blind": 2, "contextual": 5, "branded": 7},
        "serper_mode": "local_leaders",
        "allowed_schema_types": ["Dentist", "LocalBusiness", "PostalAddress", "OpeningHoursSpecification", "MedicalBusiness"],
        "allowed_blueprint_signals": ["local_trust", "appointment_conversion", "doctor_authority"],
        "must_have_signals": [
            "LocalBusiness or Dentist schema",
            "city/location mentions",
            "reviews",
            "emergency service pages",
            "doctor profiles"
        ],
        "risk_factors": [
            "missing city name",
            "weak doctor bios",
            "no trust/legal info"
        ],
        "persona_templates": [
            {
                "persona": "Emergency Patient",
                "intent": "urgent",
                "query_types": ["contextual"],
                "example_queries": [
                    "emergency dentist near me",
                    "tooth pain dentist Rome"
                ]
            },
            {
                "persona": "Family Patient",
                "intent": "trust",
                "query_types": ["contextual", "branded"],
                "example_queries": [
                    "best family dentist Rome",
                    "[clinic] reviews"
                ]
            }
        ],
        "blind_fallback_templates": {
            "it": ["miglior dentista zona", "studio dentistico convenzionato", "dentista per bambini", "urgenza dentista oggi", "pulizia denti costo"],
            "en": ["best dentist near me", "trusted dental clinic", "pediatric dentist nearby", "emergency dental appointment", "teeth cleaning cost"]
        },
        "contextual_fallback_templates": {
            "it": ["quanto costa un impianto dentale", "migliori studi dentistici per estetica", "dentista aperto sabato"],
            "en": ["cost of dental implants", "best cosmetic dentistry", "dentist open on saturday"]
        }
    },

    "local_law_firm": {
        "label": "Local Law Firm",
        "macro_industry": "Professional Services",
        "geo_behavior": "trust_authority_local",
        "query_style": "legal_authority",
        "scale_default": "Local",
        "location_enforce": True,
        "stress_test_budget": {"blind": 3, "contextual": 5, "branded": 6},
        "serper_mode": "local_leaders",
        "allowed_schema_types": ["LegalService", "LocalBusiness", "PostalAddress", "Attorney"],
        "allowed_blueprint_signals": ["legal_authority", "localized_practice_expertise"],
        "must_have_signals": [
            "practice area pages",
            "lawyer bios",
            "bar credentials",
            "location pages"
        ],
        "risk_factors": [
            "generic legal copy",
            "no named professionals",
            "weak niche specialization"
        ],
        "persona_templates": [
            {
                "persona": "Urgent Client",
                "intent": "problem resolution",
                "query_types": ["contextual"],
                "example_queries": [
                    "real estate lawyer Rome",
                    "lawyer for landlord disputes Rome"
                ]
            },
            {
                "persona": "Evaluator",
                "intent": "trust",
                "query_types": ["branded"],
                "example_queries": [
                    "[firm] reviews",
                    "[lawyer name] avvocato Roma"
                ]
            }
        ],
        "blind_fallback_templates": {
            "it": ["miglior avvocato civilista", "studio legale esperto in successioni", "avvocato penalista h24", "consulenza legale online", "avvocato per separazione"],
            "en": ["best civil lawyer", "law firm for inheritance", "criminal defense attorney h24", "online legal consultation", "divorce lawyer nearby"]
        },
        "contextual_fallback_templates": {
            "it": ["costo causa legale italia", "come scegliere un buon avvocato", "tempi processo civile"],
            "en": ["legal fees in italy", "how to choose a good lawyer", "civil trial duration"]
        }
    },

    "freelancer_consultant": {
        "label": "Freelancer / Consultant",
        "macro_industry": "Services",
        "geo_behavior": "personal_expertise",
        "query_style": "personal_expertise",
        "scale_default": "Local",
        "location_enforce": True,
        "stress_test_budget": {"blind": 4, "contextual": 5, "branded": 5},
        "serper_mode": "local_or_niche_leaders",
        "allowed_schema_types": ["Person", "LocalBusiness", "Service"],
        "allowed_blueprint_signals": ["personal_brand", "outcome_proof", "localized_presence"],
        "must_have_signals": [
            "clear personal brand",
            "case studies",
            "service pages",
            "testimonials"
        ],
        "risk_factors": [
            "anonymous site",
            "no proof of work",
            "unclear positioning"
        ],
        "persona_templates": [
            {
                "persona": "Lead",
                "intent": "solution seeking",
                "query_types": ["blind", "contextual"],
                "example_queries": [
                    "freelance copywriter for SaaS",
                    "branding consultant Rome"
                ]
            },
            {
                "persona": "Evaluator",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[name] reviews",
                    "[name] portfolio"
                ]
            }
        ],
        "blind_fallback_templates": {
            "it": ["consulente marketing esperto", "freelance per siti web", "esperto seo per pmi", "copywriter per brochure", "coach aziendale roma"],
            "en": ["expert marketing consultant", "freelance web designer", "seo expert for small business", "copywriter for brochures", "business coach nearby"]
        },
        "contextual_fallback_templates": {
            "it": ["perché assumere un consulente esterno", "costo freelance orario", "esempi di successo freelance"],
            "en": ["why hire a consultant", "freelance hourly rates", "freelance success stories"]
        }
    },

    "agency_marketing": {
        "label": "Marketing Agency",
        "macro_industry": "Services",
        "geo_behavior": "authority_case_study",
        "query_style": "case_study_authority",
        "scale_default": "National",
        "location_enforce": False,
        "stress_test_budget": {"blind": 8, "contextual": 8, "branded": 6},
        "serper_mode": "category_leaders",
        "allowed_schema_types": ["Organization", "Service", "WebSite", "FAQPage"],
        "allowed_blueprint_signals": ["industry_verticals", "case_study_anchoring", "partner_status"],
        "must_have_signals": [
            "case studies",
            "service pages",
            "team page",
            "industry specialization pages"
        ],
        "risk_factors": [
            "empty agency fluff",
            "no outcomes",
            "no specialization"
        ],
        "persona_templates": [
            {
                "persona": "Founder / CMO",
                "intent": "provider discovery",
                "query_types": ["blind", "contextual"],
                "example_queries": [
                    "best SaaS marketing agency",
                    "SEO agency for e-commerce"
                ]
            },
            {
                "persona": "Buyer",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[agency] reviews",
                    "[agency] case studies"
                ]
            }
        ],
        "blind_fallback_templates": {
            "it": ["migliore agenzia seo italia", "agenzia marketing per ecommerce", "esperti lead generation b2b", "agenzia pubblicità social", "consulenza crescita aziendale"],
            "en": ["best seo agency", "marketing agency for ecommerce", "b2b lead generation experts", "social media ad agency", "business growth consulting"]
        },
        "contextual_fallback_templates": {
            "it": ["case studies agenzie marketing eccellenti", "come scalare un ecommerce con agenzia", "migliori agenzie digitali milano"],
            "en": ["marketing agency case studies", "how to scale ecommerce with agency", "best digital agencies nearby"]
        }
    },

    "education_course_provider": {
        "label": "Education / Course Provider",
        "macro_industry": "Education",
        "geo_behavior": "outcome_trust",
        "query_style": "learning_outcomes",
        "scale_default": "National",
        "location_enforce": False,
        "stress_test_budget": {"blind": 8, "contextual": 10, "branded": 6},
        "serper_mode": "category_leaders",
        "allowed_schema_types": ["Course", "EducationEvent", "Organization", "Review"],
        "allowed_blueprint_signals": ["outcome_validation", "curriculum_depth", "instructor_trust"],
        "must_have_signals": [
            "course pages",
            "outcomes/certifications",
            "instructor bios",
            "reviews/testimonials"
        ],
        "risk_factors": [
            "no instructor proof",
            "no course outcomes",
            "generic edu promises"
        ],
        "persona_templates": [
            {
                "persona": "Learner",
                "intent": "skill acquisition",
                "query_types": ["blind", "contextual"],
                "example_queries": [
                    "best data analytics course",
                    "how to learn python online"
                ]
            },
            {
                "persona": "Evaluator",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[academy] review",
                    "[course] worth it"
                ]
            }
        ],
    },

    "media_blog": {
        "label": "Media / Blog",
        "macro_industry": "Publishing",
        "geo_behavior": "discovery_driven",
        "query_style": "thought_leadership",
        "scale_default": "National",
        "location_enforce": False,
        "stress_test_budget": {"blind": 12, "contextual": 8, "branded": 4},
        "serper_mode": "topic_authorities",
        "allowed_schema_types": ["Article", "NewsArticle", "BlogPosting", "Person"],
        "allowed_blueprint_signals": ["topical_clusters", "author_credibility", "content_freshness"],
        "must_have_signals": [
            "author pages",
            "content clusters",
            "freshness",
            "entity-rich articles"
        ],
        "risk_factors": [
            "thin editorial identity",
            "weak author trust",
            "no topical clustering"
        ],
        "persona_templates": [
            {
                "persona": "Searcher",
                "intent": "information seeking",
                "query_types": ["blind", "contextual"],
                "example_queries": [
                    "how to build muscle naturally",
                    "rome study visa tips"
                ]
            },
            {
                "persona": "Loyal Reader",
                "intent": "navigation/validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[site] review",
                    "[site] author"
                ]
            }
        ],
    },

    "restaurant_hospitality": {
        "label": "Restaurant / Hospitality",
        "macro_industry": "Hospitality",
        "geo_behavior": "experience_proximity",
        "query_style": "sensory_experiential",
        "scale_default": "Local",
        "location_enforce": True,
        "stress_test_budget": {"blind": 3, "contextual": 5, "branded": 6},
        "serper_mode": "local_leaders",
        "allowed_schema_types": ["Restaurant", "LocalBusiness", "Menu", "FoodEstablishment", "PostalAddress", "OpeningHoursSpecification"],
        "allowed_blueprint_signals": ["menu_optimization", "reservation_hooks", "local_geo_signals"],
        "must_have_signals": [
            "menu",
            "location",
            "reservation info",
            "photos",
            "reviews"
        ],
        "risk_factors": [
            "no menu text",
            "thin location context",
            "brand hidden behind Instagram only"
        ],
        "persona_templates": [
            {
                "persona": "Nearby Diner",
                "intent": "discovery",
                "query_types": ["contextual"],
                "example_queries": [
                    "migliore pasta fatta in casa",
                    "ristorante romantico centro",
                    "dove mangiare bene vicino a me"
                ]
            },
            {
                "persona": "Evaluator",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[restaurant] recensioni",
                    "[restaurant] menu"
                ]
            }
        ],
        "blind_fallback_templates": {
            "it": [
                "migliori ristoranti in zona",
                "posti dove mangiare bene all'aperto",
                "ristoranti con cucina tipica locale",
                "migliori trattorie recensite",
                "dove cenare stasera in centro"
            ],
            "en": [
                "best restaurants nearby",
                "top rated local dining",
                "best places for outdoor dinner",
                "authentic local cuisine restaurants",
                "where to eat tonight in city center"
            ]
        },
        "contextual_fallback_templates": {
            "it": [
                "menu e prezzi ristoranti tipici",
                "esperienze culinarie uniche in zona",
                "ristoranti per occasioni speciali"
            ],
            "en": [
                "menu and prices for local restaurants",
                "unique dining experiences nearby",
                "best restaurants for special occasions"
            ]
        }
    },

    "local_tech_provider": {
        "label": "Local Tech / IT Services",
        "macro_industry": "Technology Services",
        "geo_behavior": "geo_technical",
        "query_style": "geo_technical",
        "scale_default": "National",
        "location_enforce": True,
        "stress_test_budget": {"blind": 6, "contextual": 7, "branded": 6},
        "serper_mode": "regional_leaders",
        "allowed_schema_types": ["LocalBusiness", "Service", "Organization", "PostalAddress"],
        "allowed_blueprint_signals": ["technical_expertise", "localized_service_scope", "credentials_visibility"],
        "must_have_signals": [
            "service pages",
            "case studies",
            "certifications",
            "location pages"
        ],
        "risk_factors": [
            "too generic IT messaging",
            "no certifications",
            "no local trust"
        ],
        "persona_templates": [
            {
                "persona": "Operations Manager",
                "intent": "provider discovery",
                "query_types": ["blind", "contextual"],
                "example_queries": [
                    "managed IT services Rome",
                    "cybersecurity provider Lazio"
                ]
            },
            {
                "persona": "Evaluator",
                "intent": "validation",
                "query_types": ["branded"],
                "example_queries": [
                    "[company] reviews",
                    "[company] services Rome"
                ]
            }
        ],
    }
}
