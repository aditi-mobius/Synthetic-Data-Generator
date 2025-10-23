from typing import Dict, Any


class FakerManager:
    """Manages Faker instances to avoid re-creating them for each column."""
    def __init__(self, default_locale: str = "en_US"):
        from faker import Faker
        self.Faker = Faker
        self.default_locale = default_locale
        self._instances: Dict[str, Faker] = {}
        self._provider_map: Dict[str, Any] = {}

    def add_provider_for_locale(self, locale: str, provider: Any):
        """Register a custom provider to be added for a specific locale."""
        self._provider_map[locale] = provider

    def get_instance(self, locale: str | None = None) -> Any:
        """
        Get a cached Faker instance for a given locale.
        If no locale is specified, uses the default.
        """
        target_locale = locale or self.default_locale

        if target_locale not in self._instances:
            print(f"  [DEBUG] Creating new Faker instance for locale '{target_locale}'")
            instance = self.Faker(target_locale)
            
            # Add a custom provider if one is registered for this locale
            custom_provider = self._provider_map.get(target_locale)
            if custom_provider:
                instance.add_provider(custom_provider)
                print(f"  [DEBUG] Added custom provider for '{target_locale}'")

            self._instances[target_locale] = instance

        return self._instances[target_locale]
