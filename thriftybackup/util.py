

PREFIXES = {40: 'T', 30: 'G', 20: 'M', 10: 'K', 0: ' '}
EXPONENTS = {value: key for key, value in PREFIXES.items()}


def format_size(n_bytes, align=False):
    for exp, prefix in PREFIXES.items():
        if n_bytes > 2**exp:
            break
        if not align:
            prefix = prefix.strip()
    return f'{n_bytes / 2**exp:{8 if align else 0}.02f} {prefix}B'
