from datetime import date

from post_office import mail
from data_gov_my.models import PublicationType, PublicationSubtype, Publication, Subscription

type_list = [
    'agriculture', 'bc', 'bop', 'businesses', 'census', 'census_economy',
    'construction', 'cpi', 'crime', 'demography', 'gdp', 'icths', 'iip',
    'indicators', 'ipi', 'labour', 'lifetables', 'mfg', 'mining', 'mywi', 'osh',
    'ppi', 'rubber', 'sdg', 'services', 'special', 'subnational', 'summary',
    'tourism', 'trade', 'wrt'
]

type_dict = {
    'agriculture': {'type_en': 'Agriculture', 'type_bm': 'Pertanian'},
    'bc': {'type_en': 'Building Costs', 'type_bm': 'Harga Pembinaan'},
    'bop': {'type_en': 'Balance of Payments', 'type_bm': 'Imbangan Pembayaran'},
    'businesses': {'type_en': 'Businesses', 'type_bm': 'Perniagaan'},
    'census': {'type_en': 'Population Census', 'type_bm': 'Banci Penduduk'},
    'census_economy': {'type_en': 'Economic Census', 'type_bm': 'Banci Ekonomi'},
    'construction': {'type_en': 'Construction', 'type_bm': 'Pembinaan'},
    'cpi': {'type_en': 'Consumer Price Index', 'type_bm': 'Indeks Harga Pengguna'},
    'crime': {'type_en': 'Crime', 'type_bm': 'Jenayah'},
    'demography': {'type_en': 'Demography', 'type_bm': 'Demografi'},
    'gdp': {'type_en': 'Gross Domestic Product', 'type_bm': 'Keluaran Dalam Negeri Kasar'},
    'icths': {'type_en': 'ICT Use & Access', 'type_bm': 'ICT Use & Access'},
    'iip': {'type_en': 'International Investment Position',
            'type_bm': 'Kedudukan Pelaburan Antarabangsa'},
    'indicators': {'type_en': 'Economic Indicators', 'type_bm': 'Indikator Ekonomi'},
    'ipi': {'type_en': 'Industrial Production', 'type_bm': 'Pengeluaran Perindustrian'},
    'labour': {'type_en': 'Labour', 'type_bm': 'Tenaga Buruh'},
    'lifetables': {'type_en': 'Demography', 'type_bm': 'Demografi'},
    'mfg': {'type_en': 'Manufacturing', 'type_bm': 'Pembuatan'},
    'mining': {'type_en': 'Mining', 'type_bm': 'Perlombongan'},
    'mywi': {'type_en': 'Well-Being', 'type_bm': 'Kesejahteraan'},
    'osh': {'type_en': 'Occupational Accident and Disease',
            'type_bm': 'Keselamatan dan Kesihatan Pekerja'},
    'ppi': {'type_en': 'Producer Price Index', 'type_bm': 'Indeks Harga Pengeluar'},
    'rubber': {'type_en': 'Rubber', 'type_bm': 'Getah'},
    'sdg': {'type_en': 'Sustainable Development Goals', 'type_bm': 'Matlamat Pembangunan Mampan'},
    'services': {'type_en': 'Services', 'type_bm': 'Perkhidmatan'},
    'special': {'type_en': 'Special Reports', 'type_bm': 'Laporan Khas'},
    'subnational': {'type_en': 'Subnational Statistics', 'type_bm': 'Statistik Subnasional'},
    'summary': {'type_en': 'Statistical Reviews', 'type_bm': 'Sorotan Statistik'},
    'tourism': {'type_en': 'Tourism', 'type_bm': 'Pelancongan'},
    'trade': {'type_en': 'External Trade', 'type_bm': 'Perdagangan Luar'},
    'wrt': {'type_en': 'Wholesale & Retail Trade', 'type_bm': 'Perdagangan Borong & Runcit'}
}

subtype_list = [
    'agriculture_supply_util', 'bci', 'bop', 'bop_annual_dia', 'bop_annual_fdi',
    'businesses', 'census_mukim', 'census_economy', 'census_economy_accom',
    'census_economy_admin', 'census_economy_arts', 'census_economy_bumi',
    'census_economy_education', 'census_economy_electricity',
    'census_economy_employees', 'census_economy_establishment',
    'census_economy_finance', 'census_economy_fnb', 'census_economy_foreign',
    'census_economy_health', 'census_economy_ict', 'census_economy_infocomm',
    'census_economy_personal', 'census_economy_professional',
    'census_economy_property', 'census_economy_sector', 'census_economy_sme',
    'census_economy_transport', 'census_economy_water', 'census_economy_women',
    'census_economy_wrt', 'census_economy_youth', 'construction', 'cpi', 'crime',
    'bumi', 'children', 'cod', 'demography', 'matrimony', 'population', 'pwd',
    'vitalstatistics', 'women', 'capstock', 'digitalecon', 'gdp', 'gdp_advance',
    'gdp_district', 'gdp_income', 'gdp_state', 'gfcf', 'ictsa', 'msme', 'nea',
    'sports_sa', 'tourism', 'icths', 'iip', 'bts', 'mei', 'ipi', 'employment',
    'formal_wages', 'graduates', 'jobs', 'lfs', 'lfs_informal', 'productivity',
    'wages', 'lifetables', 'mfg', 'mfg_util', 'mining_png', 'mywi', 'osh', 'ppi',
    'sppi', 'rubber', 'sdg', 'services', 'sa_matrix', 'special_floods',
    'mydistrict', 'socioecon_state', 'handbook', 'lmr', 'mesr', 'msb',
    'pocketstats', 'social_bulletin', 'social_review', 'yearbook',
    'yearbook_sabah', 'yearbook_sarawak', 'fats', 'tourism_domestic',
    'tourism_domestic_state', 'trade', 'trade_annual_sbh',
    'trade_annual_services', 'trade_annual_state', 'trade_annual_swk',
    'tradeindices', 'wrt'
]

subtype_dict = {
    'agriculture_supply_util': {
        'subtype_en': 'Agriculture Supply & Utilisation',
        'subtype_bm': 'Penggunaan & Penawaran Pertanian',
        'type': 'agriculture'
    },
    'bci': {
        'subtype_en': 'Building Costs',
        'subtype_bm': 'Harga Pembinaan',
        'type': 'bc'
    },
    'bop': {
        'subtype_en': 'Balance of Payments',
        'subtype_bm': 'Imbangan Pembayaran',
        'type': 'bop'
    },
    'bop_annual_dia': {
        'subtype_en': 'Direct Investment Abroad',
        'subtype_bm': 'Pelaburan Langsung Luar Negeri',
        'type': 'bop'
    },
    'bop_annual_fdi': {
        'subtype_en': 'Foreign Direct Investment',
        'subtype_bm': 'Pelaburan Langsung Asing',
        'type': 'bop'
    },
    'businesses': {
        'subtype_en': 'Businesses',
        'subtype_bm': 'Perniagaan',
        'type': 'businesses'
    },
    'census_mukim': {
        'subtype_en': 'Population Census by Mukim/Town/Pekan',
        'subtype_bm': 'Banci Penduduk mengikut Mukim/Bandar/Pekan',
        'type': 'census'
    },
    'census_economy': {
        'subtype_en': 'Economic Census',
        'subtype_bm': 'Banci Ekonomi',
        'type': 'census_economy'
    },
    'census_economy_accom': {
        'subtype_en': 'Economic Census (Accommodation Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Penginapan)',
        'type': 'census_economy'
    },
    'census_economy_admin': {
        'subtype_en': 'Economic Census (Administrative and Support Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Sokongan dan Pentadbiran)',
        'type': 'census_economy'
    },
    'census_economy_arts': {
        'subtype_en': 'Economic Census (Arts, Entertainment and Recreation Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Seni, Hiburan dan Rekreasi)',
        'type': 'census_economy'
    },
    'census_economy_bumi': {
        'subtype_en': 'Economic Census (Bumiputera)',
        'subtype_bm': 'Banci Ekonomi (Bumiputera)',
        'type': 'census_economy'
    },
    'census_economy_education': {
        'subtype_en': 'Economic Census (Education Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Pendidikan)',
        'type': 'census_economy'
    },
    'census_economy_electricity': {
        'subtype_en': 'Economic Census (Electricity, Gas, Steam and Air Conditioning Supply)',
        'subtype_bm': 'Banci Ekonomi (Bekalan Elektrik, Gas, Wap dan Penyamanan Udara)',
        'type': 'census_economy'
    },
    'census_economy_employees': {
        'subtype_en': 'Economic Census (Employees, Salaries & Wages)',
        'subtype_bm': 'Banci Ekonomi (Pekerja, Gaji & Upah)',
        'type': 'census_economy'
    },
    'census_economy_establishment': {
        'subtype_en': 'Economic Census (Establishments)',
        'subtype_bm': 'Banci Ekonomi (Penubuhan)',
        'type': 'census_economy'
    },
    'census_economy_finance': {
        'subtype_en': 'Economic Census (Financial Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Kewangan)',
        'type': 'census_economy'
    },
    'census_economy_fnb': {
        'subtype_en': 'Economic Census (Food and Beverage Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Makanan dan Minuman)',
        'type': 'census_economy'
    },
    'census_economy_foreign': {
        'subtype_en': 'Economic Census (Foreign-Owned Establishments)',
        'subtype_bm': 'Banci Ekonomi (Penubuhan Milik Asing)',
        'type': 'census_economy'
    },
    'census_economy_health': {
        'subtype_en': 'Economic Census (Health and Social Works Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Kesihatan dan Kebajikan Sosial)',
        'type': 'census_economy'
    },
    'census_economy_ict': {
        'subtype_en': 'Economic Census (Usage of ICT by Businesses and e-Commerce)',
        'subtype_bm': 'Banci Ekonomi (Penggunaan ICT oleh Perniagaan dan e-Dagang)',
        'type': 'census_economy'
    },
    'census_economy_infocomm': {
        'subtype_en': 'Economic Census (Information and Communication Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Maklumat dan Komunikasi)',
        'type': 'census_economy'
    },
    'census_economy_personal': {
        'subtype_en': 'Economic Census (Personal Services and Others)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Peribadi dan Lain-lain)',
        'type': 'census_economy'
    },
    'census_economy_professional': {
        'subtype_en': 'Economic Census (Professional Services; Scientific and Technical)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Profesional, Saintifik dan Teknikal)',
        'type': 'census_economy'
    },
    'census_economy_property': {
        'subtype_en': 'Economic Census (Real Estate Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Harta Tanah)',
        'type': 'census_economy'
    },
    'census_economy_sector': {
        'subtype_en': 'Economic Census (Main Sectors)',
        'subtype_bm': 'Banci Ekonomi (Sektor Utama)',
        'type': 'census_economy'
    },
    'census_economy_sme': {
        'subtype_en': 'Economic Census (Profile of SMEs)',
        'subtype_bm': 'Banci Ekonomi (Profil PKS)',
        'type': 'census_economy'
    },
    'census_economy_transport': {
        'subtype_en': 'Economic Census (Transportation and Storage Services)',
        'subtype_bm': 'Banci Ekonomi (Perkhidmatan Pengangkutan dan Penyimpanan)',
        'type': 'census_economy'
    },
    'census_economy_water': {
        'subtype_en': 'Economic Census (Water Supply; Sewerage, Waste Management and Remediation Activities)',
        'subtype_bm': 'Banci Ekonomi (Bekalan Air; Pembetungan, Pengurusan Sisa dan Aktiviti Remediasi)',
        'type': 'census_economy'
    },
    'census_economy_women': {
        'subtype_en': 'Economic Census (Women Ownership)',
        'subtype_bm': 'Banci Ekonomi (Kepemilikan Wanita)',
        'type': 'census_economy'
    },
    'census_economy_wrt': {
        'subtype_en': 'Economic Census (Wholesale and Retail Trade)',
        'subtype_bm': 'Banci Ekonomi (Perdagangan Borong dan Runcit)',
        'type': 'census_economy'
    },
    'census_economy_youth': {
        'subtype_en': 'Economic Census (Youth Possesion)',
        'subtype_bm': 'Banci Ekonomi (Pemilikan Belia)',
        'type': 'census_economy'
    },
    'construction': {
        'subtype_en': 'Construction', 'subtype_bm': 'Pembinaan', 'type': 'construction'
    },
    'cpi': {
        'subtype_en': 'Consumer Price Index', 'subtype_bm': 'Indeks Harga Pengguna', 'type': 'cpi'
    },
    'crime': {
        'subtype_en': 'Crime', 'subtype_bm': 'Jenayah', 'type': 'crime'
    },
    'bumi': {
        'subtype_en': 'Bumiputera', 'subtype_bm': 'Bumiputera', 'type': 'demography'
    },
    'children': {
        'subtype_en': 'Children', 'subtype_bm': 'Kanak-Kanak', 'type': 'demography'
    },
    'cod': {
        'subtype_en': 'Causes of Death', 'subtype_bm': 'Punca Kematian', 'type': 'demography'
    },
    'demography': {'subtype_en': 'Demography', 'subtype_bm': 'Demografi', 'type': 'demography'},
    'matrimony': {'subtype_en': 'Marriage & Divorce', 'subtype_bm': 'Perkahwinan & Penceraian',
                  'type': 'demography'},
    'population': {'subtype_en': 'Current Population Estimates', 'subtype_bm': 'Anggaran Penduduk Semasa',
                   'type': 'demography'},
    'pwd': {'subtype_en': 'Persons With Disabilities', 'subtype_bm': 'Orang Kurang Upaya',
            'type': 'demography'},
    'vitalstatistics': {'subtype_en': 'Vital Statistics', 'subtype_bm': 'Perangkaan Penting',
                        'type': 'demography'},
    'women': {'subtype_en': 'Women Empowerment', 'subtype_bm': 'Pemerkasaan Wanita', 'type': 'demography'},
    'capstock': {'subtype_en': 'Capital Stock', 'subtype_bm': 'Stok Modal', 'type': 'gdp'},
    'digitalecon': {'subtype_en': 'Digital Economy', 'subtype_bm': 'Ekonomi Digital', 'type': 'gdp'},
    'gdp': {'subtype_en': 'Gross Domestic Product', 'subtype_bm': 'Keluaran Dalam Negeri Kasar', 'type': 'gdp'},
    'gdp_advance': {'subtype_en': 'Advance Gross Domestic Product',
                    'subtype_bm': 'Keluaran Dalam Negeri Kasar Awalan', 'type': 'gdp'},
    'gdp_district': {'subtype_en': 'Gross Domestic Product by District',
                     'subtype_bm': 'Keluaran Dalam Negeri Kasar mengikut Daerah', 'type': 'gdp'},
    'gdp_income': {'subtype_en': 'Gross Domestic Product, Income Approach',
                   'subtype_bm': 'Keluaran Dalam Negeri Kasar, Kaedah Pendapatan', 'type': 'gdp'},
    'gdp_state': {'subtype_en': 'Gross Domestic Product by State',
                  'subtype_bm': 'Keluaran Dalam Negeri Kasar mengikut Negeri', 'type': 'gdp'},
    'gfcf': {'subtype_en': 'Gross Fixed Capital Formation', 'subtype_bm': 'Pembentukan Modal Tetap Kasar',
             'type': 'gdp'},
    'ictsa': {'subtype_en': 'ICT Satellite Account', 'subtype_bm': 'Akaun Satelit TMK', 'type': 'gdp'},
    'msme': {'subtype_en': 'Performance of Micro, Small & Medium Enterprises (MSMEs)',
             'subtype_bm': 'Prestasi Perniagaan Mikro, Kecil & Sederhana (MSME)', 'type': 'gdp'},
    'nea': {'subtype_en': 'National Economic Accounts', 'subtype_bm': 'Akaun Ekonomi Negara', 'type': 'gdp'},
    'sports_sa': {'subtype_en': 'Sports Satellite Account', 'subtype_bm': 'Akaun Satelit Sukan', 'type': 'gdp'},
    'tourism': {'subtype_en': 'Tourism Satellite Account', 'subtype_bm': 'Akaun Satelit Pelancongan',
                'type': 'gdp'},
    'icths': {'subtype_en': 'ICT Use & Access', 'subtype_bm': 'Penggunaan & Capaian ICT', 'type': 'icths'},
    'iip': {'subtype_en': 'International Investment Position', 'subtype_bm': 'Kedudukan Pelaburan Antarabangsa',
            'type': 'iip'},
    'bts': {'subtype_en': 'Business Tendency', 'subtype_bm': 'Kecenderungan Perniagaan', 'type': 'indicators'},
    'mei': {'subtype_en': 'Economic Indicators', 'subtype_bm': 'Indikator Ekonomi', 'type': 'indicators'},
    'ipi': {'subtype_en': 'Industrial Production', 'subtype_bm': 'Pengeluaran Perindustrian', 'type': 'ipi'},
    'employment': {'subtype_en': 'Employment', 'subtype_bm': 'Guna Tenaga', 'type': 'labour'},
    'formal_wages': {'subtype_en': 'Formal Sector Wages', 'subtype_bm': 'Gaji Sektor Formal', 'type': 'labour'},
    'graduates': {'subtype_en': 'Graduates', 'subtype_bm': 'Graduan', 'type': 'labour'},
    'jobs': {'subtype_en': 'Job Market Insights', 'subtype_bm': 'Analisa Pasaran Buruh', 'type': 'labour'},
    'lfs': {'subtype_en': 'Labour Force Survey', 'subtype_bm': 'Survei Tenaga Buruh', 'type': 'labour'},
    'lfs_informal': {'subtype_en': 'Informal Sector Workforce', 'subtype_bm': 'Tenaga Buruh Tidak Formal',
                     'type': 'labour'},
    'productivity': {'subtype_en': 'Labour Productivity', 'subtype_bm': 'Produktiviti Tenaga Buruh',
                     'type': 'labour'},
    'wages': {'subtype_en': 'Salaries & Wages', 'subtype_bm': 'Gaji & Upah', 'type': 'labour'},
    'lifetables': {'subtype_en': 'Abridged Life Tables', 'subtype_bm': 'Jadual Hayat Ringkas',
                   'type': 'lifetables'},
    'mfg': {'subtype_en': 'Manufacturing', 'subtype_bm': 'Pembuatan', 'type': 'mfg'},
    'mfg_util': {'subtype_en': 'Manufacturing Capacity Utilisation',
                 'subtype_bm': 'Penggunaan Kapisiti Pembuatan', 'type': 'mfg'},
    'mining_png': {'subtype_en': 'Mining (Petroleum & Natural Gas)',
                   'subtype_bm': 'Perlombongan (Petroleum & Gas Asli)', 'type': 'mining'},
    'mywi': {'subtype_en': 'Well-Being', 'subtype_bm': 'Kesejahteraan', 'type': 'mywi'},
    'osh': {'subtype_en': 'Occupational Accident and Disease',
            'subtype_bm': 'Keselamatan dan Kesihatan Pekerja', 'type': 'osh'},
    'ppi': {'subtype_en': 'Producer Price Index', 'subtype_bm': 'Indeks Harga Pengeluar', 'type': 'ppi'},
    'sppi': {'subtype_en': 'Services Producer Price Index', 'subtype_bm': 'Indeks Harga Pengeluar Perkhidmatan',
             'type': 'ppi'}, 'rubber': {'subtype_en': 'Rubber', 'subtype_bm': 'Getah', 'type': 'rubber'},
    'sdg': {'subtype_en': 'Sustainable Development Goals', 'subtype_bm': 'Matlamat Pembangunan Mampan',
            'type': 'sdg'},
    'services': {'subtype_en': 'Services', 'subtype_bm': 'Perkhidmatan', 'type': 'services'},
    'sa_matrix': {'subtype_en': 'Social Accounting Matrix', 'subtype_bm': 'Matriks Akaun Sosial',
                  'type': 'special'},
    'special_floods': {'subtype_en': 'Impact of Floods', 'subtype_bm': 'Impak Banjir', 'type': 'special'},
    'mydistrict': {'subtype_en': 'MyDistrict', 'subtype_bm': 'MyDistrict', 'type': 'subnational'},
    'socioecon_state': {'subtype_en': 'Socioeconomy by State', 'subtype_bm': 'Sosioekonomi mengikut Negeri',
                        'type': 'subnational'},
    'handbook': {'subtype_en': 'Statistics Handbook', 'subtype_bm': 'Buku Panduan Statistik',
                 'type': 'summary'},
    'lmr': {'subtype_en': 'Labour Market Review', 'subtype_bm': 'Sorotan Pasaran Buruh', 'type': 'summary'},
    'mesr': {'subtype_en': 'Economic Statistics Review', 'subtype_bm': 'Sorotan Statistik Ekonomi',
             'type': 'summary'},
    'msb': {'subtype_en': 'Statistical Bulletin', 'subtype_bm': 'Buletin Statistik', 'type': 'summary'},
    'pocketstats': {'subtype_en': 'Pocket Stats', 'subtype_bm': 'Pocket Stats', 'type': 'summary'},
    'social_bulletin': {'subtype_en': 'Social Statistical Bulletin', 'subtype_bm': 'Buletin Statistik Sosial',
                        'type': 'summary'},
    'social_review': {'subtype_en': 'Social Statistics Review', 'subtype_bm': 'Sorotan Statistik Sosial',
                      'type': 'summary'},
    'yearbook': {'subtype_en': 'Statistics Yearbook', 'subtype_bm': 'Buku Tahunan Statistik',
                 'type': 'summary'}, 'yearbook_sabah': {'subtype_en': 'Statistics Yearbook for Sabah',
                                                        'subtype_bm': 'Buku Tahunan Statistik bagi Sabah',
                                                        'type': 'summary'},
    'yearbook_sarawak': {'subtype_en': 'Statistics Yearbook for Sarawak',
                         'subtype_bm': 'Buku Tahunan Statistik bagi Sarawak', 'type': 'summary'},
    'fats': {'subtype_en': 'Foreign Affiliates Abroad', 'subtype_bm': 'Affiliate Malaysia di Luar Negeri',
             'type': 'tourism'},
    'tourism_domestic': {'subtype_en': 'Domestic Tourism', 'subtype_bm': 'Pelancongan Domestik',
                         'type': 'tourism'},
    'tourism_domestic_state': {'subtype_en': 'Domestic Tourism by State',
                               'subtype_bm': 'Pelancongan Domestik mengikut Negeri', 'type': 'tourism'},
    'trade': {'subtype_en': 'External Trade', 'subtype_bm': 'Perdagangan Luar', 'type': 'trade'},
    'trade_annual_sbh': {'subtype_en': 'External Trade of Sabah', 'subtype_bm': 'Perdagangan Luar Sabah',
                         'type': 'trade'}, 'trade_annual_services': {'subtype_en': 'External Trade in Services',
                                                                     'subtype_bm': 'Perdagangan Luar Perkhidmatan',
                                                                     'type': 'trade'},
    'trade_annual_state': {'subtype_en': 'External Trade by State',
                           'subtype_bm': 'Perdagangan Luar mengikut Negeri', 'type': 'trade'},
    'trade_annual_swk': {'subtype_en': 'External Trade of Sarawak', 'subtype_bm': 'Perdagangan Luar Sarawak',
                         'type': 'trade'},
    'tradeindices': {'subtype_en': 'External Trade Indices', 'subtype_bm': 'Indeks Perdagangan Luar',
                     'type': 'trade'},
    'wrt': {'subtype_en': 'Wholesale & Retail Trade', 'subtype_bm': 'Perdagangan Borong & Runcit',
            'type': 'wrt'}
}


def populate_publication_types():
    for k in type_dict:
        PublicationType.objects.get_or_create(
            id=k,
            type_en=type_dict[k]['type_en'],
            type_bm=type_dict[k]['type_bm']
        )


def populate_publication_subtypes():
    for k in subtype_dict:
        publication_type, created = PublicationType.objects.get_or_create(id=subtype_dict[k]['type'])
        PublicationSubtype.objects.get_or_create(
            id=k,
            publication_type=publication_type,
            subtype_bm=subtype_dict[k]['subtype_bm'],
            subtype_en=subtype_dict[k]['subtype_en'], )


def craft_template_en(publication_id, publication_type_title, description):
    description = description[0].lower() + description[1:]
    if description[-1] == '.':
        description = description[:-1]
    return f"""
The Department of Statistics Malaysia (DOSM) has released the latest data and analysis of the {publication_type_title}. The publication contains {description}.

You may access the publication at this link:
https://open.dosm.gov.my/publications/{publication_id}

If you have any questions about the data, you may write to data@dosm.gov.my with your enquiry.

Warm regards,
OpenDOSM Notification Bot

Note: To stop or amend your OpenDOSM notifications, go to: https://open.dosm.gov.my/publications/subscribe

--------
"""


def craft_template_bm(publication_id, publication_type_title, description):
    description = description[0].lower() + description[1:]
    if description[-1] == '.':
        description = description[:-1]
    return f'''
Jabatan Perangkaan Malaysia (DOSM) telah menerbitkan data dan analisis terkini bagi {publication_type_title}. Penerbitan ini mengandungi {description}.

Anda boleh mengakses penerbitan tersebut di pautan ini:
https://open.dosm.gov.my/publications/{publication_id}

Sekiranya anda ada sebarang pertanyaan mengenai data tersebut, anda boleh menghantar enkuiri kepada data@dosm.gov.my.

Sekian, terima kasih.

Bot Notifikasi OpenDOSM

Nota: Untuk menghentikan atau meminda notifikasi anda daripada OpenDOSM, sila ke: https://open.dosm.gov.my/publications/subscribe
'''

def craft_title(title):
    # Extract the month/year within square brackets
    period = title.split(']')[0].strip('[')
    topic = title.split(']')[1]
    new_title = f"{topic}: {period}"
    return new_title

def send_email_to_subscribers():
    # TODO: get_preferred_language()
    today = date.today()
    publications_today = Publication.objects.filter(
        release_date__year=today.year,
        release_date__month=today.month,
        release_date__day=today.day,
        language='en-GB'  # TODO: Get it dynamically
    )

    for p in publications_today:
        subscribers_email = [
            s.email for s in Subscription.objects.filter(publications__contains=[p.publication_type])
        ]

        for s in subscribers_email:
            mail.send(
                sender='notif@opendosm.my',
                recipients=[s],
                subject=craft_title(p.title),
                message=craft_template_en(p.publication_id, p.title, p.description),
                priority='now'
            )

def create_token_message(jwt_token):
    # TODO: handle language preference
    token_message_en = f"""
<p>Thank you for subscribing!</p>

<p>Please use the following token to authenticate your email on OpenDOSM:</p>

<p>{jwt_token}</p>

<p>Warm regards,<br>OpenDOSM Authentication Bot</p>
<hr>
<i><p>Tech FAQ: Why such a long token?</p>

<p>Most websites send a simple 6-digit OTP for email verification. However, this approach requires extra resources (like a database) to store and track each OTP.</p>

<p>Therefore, we use JSON Web Tokens (JWTs) instead. These tokens are long enough to be self-contained and cryptographically secure, meaning they don’t require extra server resources. This makes OpenDOSM faster and more secure for you!</p></i>
"""
    return token_message_en

class OpenDosmMail:
    def __init__(self):
        pass