def cleanSubmittedSample(sample: str):
    sample = sample.lower().strip()

    # special case
    if sample == 'unk' or sample == '':
        return 'unknown'
    if 'open wound' in sample:
        return 'opened wound'
    if 'bited wound' in sample:
        return 'bite wound'

    # ตัดวงเล็บ, /, at, or
    cut_list1 = ["(", "/", '@', ' at ', ' or ']
    for cut in cut_list1:
        if cut in sample:
            sample = sample[:sample.index(cut)].strip()

    # ตัด right, left
    cut_list2 = ['right ', 'rt.', 'r.', 'rt ', 'left ', 'lt.', 'l.', 'lt ']
    for cut in cut_list2:
        if cut in sample:
            sample = sample.replace(cut, '').strip()

    return sample
