from faker.providers import BaseProvider


class TransliteratedArabicProvider(BaseProvider):
    """
    A Faker provider for transliterated Arabic names.
    """

    first_names_male = (
        'Mohammed', 'Ahmed', 'Ali', 'Omar', 'Youssef', 'Khaled', 'Abdullah',
        'Tariq', 'Hassan', 'Ibrahim', 'Said', 'Jamal', 'Fahd', 'Sultan',
        'Faisal', 'Majid', 'Rashid', 'Nasser', 'Salim', 'Zayn', 'Mustafa',
        'Tarek', 'Karim', 'Amir', 'Rami', 'Samer', 'Ziad', 'Bilal', 'Fadi',
        'Hadi', 'Jamil', 'Kamal', 'Nabil', 'Raed', 'Walid', 'Yasir', 'Adel',
        'Faris', 'Ghassan', 'Hamza', 'Imad', 'Jafar', 'Khalid', 'Marwan',
        'Nader', 'Osama', 'Qasim', 'Riad', 'Sharif', 'Usama', 'Wael', 'Yahya', 'Zaki'
    )

    first_names_female = (
        'Fatima', 'Aisha', 'Zainab', 'Mariam', 'Nour', 'Huda', 'Layla', 'Amina',
        'Sara', 'Yasmin', 'Rania', 'Dina', 'Hala', 'Jana', 'Lina', 'Mona',
        'Nadia', 'Reem', 'Salma', 'Farah', 'Amal', 'Basma', 'Dalal', 'Eman',
        'Ghada', 'Hanan', 'Iman', 'Jumanah', 'Khadija', 'Lama', 'Maha', 'Nawal',
        'Ola', 'Qamar', 'Rawan', 'Samira', 'Tahani', 'Wafa', 'Yara', 'Zahra'
    )

    last_names = (
        'Al-Fahd', 'Al-Sultan', 'Al-Saud', 'Al-Rashid', 'Al-Mansoori',
        'Khan', 'Hussain', 'Al-Jaber', 'Al-Marzouqi', 'Al-Mazrouei',
        'Al-Qasimi', 'Al-Nuaimi', 'Al-Shamsi', 'Al-Ketbi', 'Bin Zayed',
        'Haddad', 'Abbasi', 'Darwish', 'Ghanem', 'Hamdan', 'Jaber', 'Khoury',
        'Mansour', 'Nasser', 'Othman', 'Qureshi', 'Saleh', 'Taleb', 'Younis',
        'Zayed', 'Abadi', 'Bishara', 'Daher', 'Fadel', 'Ghattas', 'Hijazi',
        'Ishaq', 'Jradi', 'Kanaan', 'Lutfi', 'Malek', 'Nimer', 'Obaid', 'Qasem'
    )

    def name_male(self) -> str:
        """
        Returns a transliterated male name.
        """
        first = self.random_element(self.first_names_male)
        last = self.random_element(self.last_names)
        return f"{first} {last}"

    def name_female(self) -> str:
        """
        Returns a transliterated female name.
        """
        first = self.random_element(self.first_names_female)
        last = self.random_element(self.last_names)
        return f"{first} {last}"

    def first_name_male(self) -> str:
        """
        Returns a transliterated male first name.
        """
        return self.random_element(self.first_names_male)

    def first_name_female(self) -> str:
        """
        Returns a transliterated female first name.
        """
        return self.random_element(self.first_names_female)

    def last_name(self) -> str:
        """
        Returns a transliterated last name.
        """
        return self.random_element(self.last_names)
