from __future__ import annotations

def _srgb_to_linear(c: float) -> float:
    return c/12.92 if c <= 0.03928 else ((c+0.055)/1.055) ** 2.4

def _hex_to_rgb(hexstr: str):
    h = hexstr.lstrip("#")
    if len(h) != 6: raise ValueError("bad hex")
    return tuple(int(h[i:i+2], 16) for i in (0,2,4))

def _luminance(rgb):
    r, g, b = [x/255.0 for x in rgb]
    def to_lin(x): return x/12.92 if x <= 0.03928 else ((x+0.055)/1.055) ** 2.4
    r_lin, g_lin, b_lin = to_lin(r), to_lin(g), to_lin(b)
    return 0.2126*r_lin + 0.7152*g_lin + 0.0722*b_lin

def contrast_ratio(hex1: str, hex2: str) -> float:
    L1 = _luminance(_hex_to_rgb(hex1))
    L2 = _luminance(_hex_to_rgb(hex2))
    light = max(L1, L2); dark = min(L1, L2)
    return (light + 0.05) / (dark + 0.05)

def meets_wcag_aa(ratio: float, large_text: bool=False) -> bool:
    return ratio >= (3.0 if large_text else 4.5)
