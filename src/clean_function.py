# clean data function (species / sir / bact_genus / submitted_sample)
import numpy as np


def cleanSpecies(species: str):
    species = species.lower().strip()
    if species == 'cat' or species == 'dog':
        return species
    return 'other'


def cleanSIR(sir: str):
    sir = sir.upper().strip()
    if sir == 'S' or sir == 'R' or sir == 'I' or sir == '+' or sir == '-':
        return sir
    if sir == 'NEG':
        return '-'
    if sir == 'POS':
        return '+'
    return np.nan


def cleanBactGenus(bact_species: str):
    bact_species = bact_species.lower().strip()
    bact_genus_list = ['staphylococcus', 'escherichia', 'pseudomonas', 'enterococcus',
                       'proteus', 'klebsiella', 'acinetobacter', 'pasteurella', 'enterobacter',
                       'kocuria', 'burkholderia', 'micrococcus', 'serratia', 'bacillus',
                       'sphingomonas', 'aeromonas', 'unknown', 'corynebacterium',
                       'citrobacter', 'stenotrophomonas', 'morganella', 'leuconostoc',
                       'providencia', 'granulicatella', 'dermacoccus', 'achromobacter',
                       'pantoea', 'salmonella', 'neisseria', 'aerococcus', 'gardnerella',
                       'rothia', 'moraxella', 'myroides', 'rhizobium', 'lactococcus',
                       'gemella', 'candida', 'alloiococcus', 'ochrobactrum', 'actinomyces',
                       'chryseobacterium', 'pediococcus', 'yersinia', 'elizabethkingia',
                       'paenibacillus', 'vibrio', 'roseomonas', 'brevibacterium',
                       'alcaligenes', 'rhodococcus', 'lysinibacillus', 'brevibacillus',
                       'massilia', 'cupriavidus', 'trueperella', 'paenbacillus', 'bordetella',
                       'methylobacterium', 'kytococcus', 'sphingobacterium', 'nocardia',
                       'raoultella', 'novosphingobium', 'cutibacterium', 'macrococcus',
                       'brevundimonas', 'vagococcus', 'pentoea', 'ralstonia', 'erysipelothrix',
                       'shigella', 'cellulosimicrobium', 'ignatzschineria', 'mycobacterium',
                       'lactobacillus', 'actinobacillus', 'hafnia', 'tsukamurella',
                       'chromobacterium', 'exiguobacterium', 'facklamia', 'mycoplasma',
                       'globicatella', 'shewanella', 'agromyces', 'microbacterium']

    if 'staphylococci' in bact_species in bact_species:
        return 'staphylococcus'

    for bact in bact_genus_list:
        if bact in bact_species:
            return bact
    # if 'non or low reactive biopattern' in bact_species or 'unidentified' in bact_species:
    #     return 'unknown'
    return 'unknown'


def cleanSubmittedSample(sample: str, vitek_id: str):
    submitted_sample_list_gp = eval(open(
        "./ml_model/schema/submitted_sample_binning_gp.txt", 'r', encoding='utf-8').read())
    submitted_sample_list_gn = eval(open(
        "./ml_model/schema/submitted_sample_binning_gn.txt", 'r', encoding='utf-8').read())
    sample = sample.lower().strip()
    vitek_id = vitek_id.lower().strip()
    if sample == 'unk' or sample == '':
        return 'unknown'

    # ตัดวงเล็บ, /, at, or
    cut_list1 = ["(", "/", ' at ', '@', ' or ']
    for cut in cut_list1:
        if cut in sample:
            sample = sample[:sample.index(cut)].strip()

    # ตัด right, left
    cut_list2 = ['right ', 'rt.', 'r.', 'rt ', 'left ', 'lt.', 'l.', 'lt ']
    for cut in cut_list2:
        if cut in sample:
            sample = sample.replace(cut, '').strip()

    # special case
    if sample == 'ear':
        return 'ear'
    if 'open wound' in sample:
        return 'opened wound'
    if 'bited wound' in sample:
        return 'bite wound'
    if 'ear cerumen' in sample:
        return 'ear wax'

    # clean
    if vitek_id == 'gp':
        startswith_list = submitted_sample_list_gp
        contains_list = ['bite wound', 'nasal cavity', 'opened wound']
    else:
        startswith_list = submitted_sample_list_gn
        contains_list = ['bite wound', 'nasal cavity',
                         'nasal mucosa', 'opened wound']

    for submitted_sample in startswith_list:
        if sample.startswith(submitted_sample):
            return submitted_sample

    for submitted_sample in contains_list:
        if submitted_sample in sample:
            return submitted_sample

    return 'other'

def cleanSubmittedSampleCategory(sample: str, vitek_id: str):
    submitted_sample_list_gp = eval(open(
        "./ml_model/schema/submitted_sample_binning_gp.txt", 'r', encoding='utf-8').read())
    submitted_sample_list_gn = eval(open(
        "./ml_model/schema/submitted_sample_binning_gn.txt", 'r', encoding='utf-8').read())
    sample = sample.lower().strip()
    vitek_id = vitek_id.lower().strip()
    
    # clean
    if vitek_id == 'gp':
        startswith_list = submitted_sample_list_gp
    else:
        startswith_list = submitted_sample_list_gn

    for submitted_sample in startswith_list:
        if sample.startswith(submitted_sample):
            return submitted_sample

    return 'xxrare'
