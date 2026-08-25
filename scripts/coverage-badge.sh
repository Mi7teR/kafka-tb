#!/usr/bin/env bash
#
# Writes a coverage badge as a self-contained SVG. No third-party service is
# involved: the file is generated here, published to the "badges" branch by CI,
# and served by raw.githubusercontent. Codecov was tried first and did not work.
#
# Usage: scripts/coverage-badge.sh <percent> <output.svg>
set -euo pipefail

pct="${1:?usage: coverage-badge.sh <percent> <output.svg>}"
out="${2:?usage: coverage-badge.sh <percent> <output.svg>}"

# Shields' own thresholds, so the colour means what a reader expects it to.
whole="${pct%%.*}"
if   [ "$whole" -ge 90 ]; then colour="#4c1"      # brightgreen
elif [ "$whole" -ge 80 ]; then colour="#a3c51c"   # yellowgreen
elif [ "$whole" -ge 70 ]; then colour="#dfb317"   # yellow
elif [ "$whole" -ge 50 ]; then colour="#fe7d37"   # orange
else                           colour="#e05d44"   # red
fi

label="coverage"
value="${pct}%"
# 6.5px per character is close enough to Verdana 11 for these two short strings.
label_w=$(( ${#label} * 7 + 10 ))
value_w=$(( ${#value} * 7 + 10 ))
total_w=$(( label_w + value_w ))

cat > "$out" <<SVG
<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="${total_w}" height="20" role="img" aria-label="${label}: ${value}">
  <title>${label}: ${value}</title>
  <linearGradient id="s" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <clipPath id="r"><rect width="${total_w}" height="20" rx="3" fill="#fff"/></clipPath>
  <g clip-path="url(#r)">
    <rect width="${label_w}" height="20" fill="#555"/>
    <rect x="${label_w}" width="${value_w}" height="20" fill="${colour}"/>
    <rect width="${total_w}" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="110">
    <text x="$(( label_w * 5 ))" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="$(( (label_w - 10) * 10 ))">${label}</text>
    <text x="$(( label_w * 5 ))" y="140" transform="scale(.1)" textLength="$(( (label_w - 10) * 10 ))">${label}</text>
    <text x="$(( (label_w * 10 + value_w * 5) ))" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="$(( (value_w - 10) * 10 ))">${value}</text>
    <text x="$(( (label_w * 10 + value_w * 5) ))" y="140" transform="scale(.1)" textLength="$(( (value_w - 10) * 10 ))">${value}</text>
  </g>
</svg>
SVG
echo "wrote $out (${value}, ${colour})"
