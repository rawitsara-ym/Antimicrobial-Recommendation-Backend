# clean data function (species / sir / bact_genus / submitted_sample)
import numpy as np

def cleanSpecies(species:str):
    species = species.lower().strip()
    if species == 'cat' or species == 'dog':
        return species
    return 'other'

def cleanSIR(sir:str):
    sir = sir.lower().strip()
    if sir == 's' or sir == 'i' or sir == 'r' or sir == '+' or sir == '-':
        return sir
    if sir == 'neg':
        return '-'
    if sir == 'pos':
        return '+'
    return np.nan

def cleanBactGenus(bact_species:str):
    bact_species = bact_species.lower().strip()
    bact_genus_list = ['staphylococcus', 'escherichia', 'pseudomonas', 'enterococcus',
       'klebsiella', 'proteus', 'streptococcus', 'pasteurella',
       'acinetobacter', 'enterobacter', 'kocuria', 'burkholderia',
       'micrococcus', 'serratia', 'sphingomonas', 'aeromonas', 'bacillus',
       'unknown', 'leuconostoc', 'morganella', 'stenotrophomonas',
       'granulicatella', 'citrobacter', 'dermacoccus', 'providencia',
       'achromobacter', 'pantoea', 'salmonella', 'corynebacterium',
       'aerococcus', 'neisseria', 'gardnerella', 'myroides', 'lactococcus',
       'gemella', 'alloiococcus', 'rhizobium', 'moraxella', 'ochrobactrum',
       'rothia', 'candida', 'yersinia', 'chryseobacterium', 'alcaligenes',
       'pediococcus', 'paenibacillus', 'brevibacterium', 'actinomyces',
       'trueperella', 'sphingobacterium', 'bordetella', 'cupriavidus',
       'elizabethkingia', 'roseomonas', 'vibrio', 'chromobacterium',
       'mycobacterium', 'lysinibacillus', 'vagococcus', 'macrococcus',
       'nocardia', 'hafnia', 'kytococcus', 'rhodococcus', 'methylobacterium',
       'massilia', 'actinobacillus', 'ralstonia', 'erysipelothrix',
       'globicatella', 'shewanella', 'facklamia', 'raoultella', 'shigella',
       'pentoea', 'exiguobacterium']
    
    if 'staphylococci' in bact_species in bact_species:
        return 'staphylococcus'
    
    for bact in bact_genus_list:
        if bact in bact_species:
            return bact
    # if 'non or low reactive biopattern' in bact_species or 'unidentified' in bact_species:
    #     return 'unknown'
    return 'unknown'

    
def cleanSubmittedSample(sample:str, vitek_id:str):
    sample = sample.lower().strip()
    vitek_id = vitek_id.lower().strip()
    if sample == 'unk' or sample == '':
        return 'unknown'
    # ตัดวงเล็บ
    if "(" in sample:
        sample = sample[:sample.index('(')].strip()
    # ตัด right / left    
    cut_list = ['right ', 'rt.', 'rt ', 'left ', 'lt.', 'lt ']
    for cut in cut_list:
        if cut in sample:
            sample = sample.replace(cut, '').strip()
    # special case
    if sample == 'ear':
        return 'ear'
    if "opened wound" in sample:
        return "open wound"
    # clean
    if vitek_id == 'gp':
        startswith_list = ['wound', 'urine', 'unknown', 'ub mucosa', 'swab', 'surgical wound',
            'surgical site', 'superficial spreading pyoderma', 'skin swab', 'skin',
            'screw', 'pyoderma', 'pus', 'purulent', 'prostate', 'pleural effusion',
            'papule', 'other', 'open wound', 'nasal discharge', 'nasal cavity',
            'mass', 'folliculitis', 'fluid', 'fistula', 'exudate',
            'epidermal collarette', 'ear wax', 'ear swab', 'ear exudate', 'ear',
            'deep pyoderma', 'deep exudate', 'crust from skin', 'crust',
            'chronic wound', 'blood', 'bite wound', 'abscess', 'abdominal fluid',
            'abdominal effusion', 'abdominal cavity', 'abdomen']
        contains_list = ['bite wound', 'nasal cavity', 'open wound']
    else:
        startswith_list = ['wound', 'urine', 'ub mucosa', 'swab', 'surgical site', 'pus',
            'purulent', 'prostate', 'pleural effusion', 'pg abscess', 'other',
            'open wound', 'nasal mucosa', 'nasal discharge', 'nasal cavity', 'mass',
            'fluid', 'fistula', 'exudate', 'ear wax', 'ear swab', 'ear purulent',
            'ear exudate', 'ear canal', 'ear', 'chronic wound', 'bite wound',
            'abscess', 'abdominal fluid', 'abdominal effusion', 'abdominal cavity',
            'abdomen']
        contains_list = ['bite wound', 'nasal cavity', 'nasal mucosa', 'open wound']
        
    for submitted_sample in startswith_list:        
        if sample.startswith(submitted_sample):
            return submitted_sample
        
    for submitted_sample in contains_list:        
        if submitted_sample in sample:
            return submitted_sample
        
    return 'other'