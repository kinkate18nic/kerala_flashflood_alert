---
name: Kerala Flash-Flood Watch
description: Public decision-support dashboard for flood attention, source transparency, and calm operational reading.
colors:
  neutral-bg: "#efe8d8"
  neutral-bg-deep: "#d9e2d4"
  neutral-sand-light: "#f8f5ef"
  neutral-ink: "#16261f"
  neutral-muted: "#4f645a"
  neutral-panel: "#fffcf5db"
  neutral-panel-strong: "#fff9eef5"
  neutral-line: "#16261f1a"
  primary-accent: "#214f3f"
  level-normal: "#355447"
  level-watch: "#ae8115"
  level-alert: "#ba5b21"
  level-severe: "#a12b18"
  level-severe-reviewed: "#66170d"
typography:
  display:
    fontFamily: "Newsreader, Georgia, serif"
    fontSize: "clamp(2.3rem, 4vw, 4.2rem)"
    fontWeight: 700
    lineHeight: 0.96
  headline:
    fontFamily: "Newsreader, Georgia, serif"
    fontSize: "clamp(1.1rem, 1.9vw, 1.45rem)"
    fontWeight: 500
    lineHeight: 1.2
  title:
    fontFamily: "Manrope, Segoe UI, sans-serif"
    fontSize: "1.05rem"
    fontWeight: 700
    lineHeight: 1.2
  body:
    fontFamily: "Manrope, Segoe UI, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.6
  label:
    fontFamily: "Manrope, Segoe UI, sans-serif"
    fontSize: "0.86rem"
    fontWeight: 700
    lineHeight: 1.2
    letterSpacing: "0.04em"
rounded:
  sm: "12px"
  md: "18px"
  lg: "24px"
  xl: "28px"
  pill: "999px"
spacing:
  sm: "8px"
  md: "12px"
  lg: "18px"
  xl: "22px"
  xxl: "28px"
components:
  nav-pill-default:
    backgroundColor: "#ffffffb8"
    textColor: "{colors.neutral-ink}"
    typography: "{typography.label}"
    rounded: "{rounded.pill}"
    padding: "0 14px"
    height: "38px"
  nav-pill-active:
    backgroundColor: "{colors.primary-accent}"
    textColor: "#ffffff"
    typography: "{typography.label}"
    rounded: "{rounded.pill}"
    padding: "0 14px"
    height: "38px"
  status-chip:
    backgroundColor: "{colors.primary-accent}"
    textColor: "#f6f4ef"
    typography: "{typography.label}"
    rounded: "{rounded.pill}"
    padding: "0 14px"
    height: "38px"
  surface-panel:
    backgroundColor: "{colors.neutral-panel}"
    textColor: "{colors.neutral-ink}"
    rounded: "{rounded.xl}"
    padding: "22px"
  surface-card:
    backgroundColor: "{colors.neutral-panel-strong}"
    textColor: "{colors.neutral-ink}"
    rounded: "{rounded.lg}"
    padding: "18px"
---

# Design System: Kerala Flash-Flood Watch

## 1. Overview

**Creative North Star: "The Calm Signal Desk"**

This system treats the dashboard like a civic reading surface for live risk, not a generic software console and not a bureaucratic document dump. Its visual language is warm, grounded, and editorial enough to feel public-facing, but structured enough to support fast operational scanning. The page should feel like a calm desk where signals are arranged for judgment, not a command center trying to impress you.

The current implementation gets there through warm parchment backgrounds, deep green ink, serif-led headlines, and soft layered panels that separate information without turning the page into a grid of hard enterprise cards. The rhythm is intentionally quiet: round surfaces, dense but readable data groupings, and restrained emphasis that lets `Watch`, `Alert`, and `Severe` colors carry meaning without taking over the whole interface.

This system explicitly rejects the anti-references in [PRODUCT.md](/C:/Users/nisha/AndroidStudioProjects/Flashflood%20Alert/PRODUCT.md): it should not feel like a government PDF portal, a generic SaaS dashboard, an alarmist disaster app, or a newsfeed clone.

**Key Characteristics:**
- Warm civic-editorial palette instead of cold system neutrals
- Calm hierarchy with serif headlines and highly readable sans body text
- Tonal layering first, shadows secondary
- Rounded, humane surfaces rather than rigid dashboards
- Color used as status evidence, never as decorative panic

## 2. Colors

The palette is built around warm paper neutrals and deep monsoon greens, with hazard colors reserved for level semantics instead of broad decoration.

### Primary
- **Monsoon Desk Green** (`#214f3f`): the main civic accent for active navigation, chips, links, and low-key emphasis. It carries trust and utility without becoming corporate teal.

### Secondary
- **Watch Amber** (`#ae8115`): used specifically for `Watch` status communication. It should read as caution, not spectacle.
- **Alert Clay** (`#ba5b21`): used for `Alert` states and stronger operational emphasis.
- **Severe Rust** (`#a12b18`): reserved for severe review-required states where elevated urgency is legitimate.

### Tertiary
- **Reviewed Maroon** (`#66170d`): a darker confirmation variant for manually reviewed severe states.
- **Normal Moss** (`#355447`): neutral-positive level color for baseline `Normal` status.

### Neutral
- **Parchment Field** (`#efe8d8`): the base page background, warm and tactile rather than clinical white.
- **Mist Basin** (`#d9e2d4`): the deeper background stop used to cool the lower page and support long scrolling surfaces.
- **Dry Paper Lift** (`#f8f5ef`): the light mid-stop that keeps the page breathable.
- **Forest Ink** (`#16261f`): primary text and linework color, dark enough for strong contrast without using pure black.
- **Rain Note** (`#4f645a`): secondary text, helper copy, and descriptive metadata.
- **Glass Paper Panel** (`#fffcf5db`): panel surface for primary containers, with transparency to let the field texture show through.
- **Pressed Paper Card** (`#fff9eef5`): stronger card surface for evidence blocks and risk cards.
- **Soft Divider** (`#16261f1a`): low-contrast border and grid rule color.

### Named Rules
**The Status-Only Heat Rule.** Amber, orange, and red are for operational level semantics first. They should not be reused as decorative accent colors in neutral interface chrome.

**The Warm Utility Rule.** Core surfaces stay warm and paper-tinted. Do not drift into pure white, charcoal, or glossy blue-gray software neutrals.

## 3. Typography

**Display Font:** Newsreader (with Georgia fallback)  
**Body Font:** Manrope (with Segoe UI fallback)  
**Label/Mono Font:** Manrope for UI labels, Roboto Mono or Consolas for raw technical strings

**Character:** The pairing is editorial but disciplined. Newsreader gives the system public-facing seriousness and humane gravity, while Manrope keeps data, controls, and evidence blocks clear under pressure.

### Hierarchy
- **Display** (700, `clamp(2.3rem, 4vw, 4.2rem)`, `0.96`): used for the hero headline and other major framing moments. It should feel compressed, confident, and immediately legible.
- **Headline** (500, `clamp(1.1rem, 1.9vw, 1.45rem)`, `1.2`): used for supporting deck copy under the main headline and similar bridge text.
- **Title** (700, `1.05rem`, `1.2`): used for card titles, source headings, and compact section-level titles inside data surfaces.
- **Body** (400, `1rem`, `1.6`): used for explanatory copy, evidence summaries, and most paragraph text. Body measure should stay around `64ch`, which aligns well with the current implementation.
- **Label** (700, `0.86rem`, `1.2`, `0.04em`): used for pills, level markers, and compact UI metadata where decisiveness matters more than prose rhythm.

### Named Rules
**The Two Voices Rule.** Serif type frames the public meaning of the dashboard, sans type carries the working interface. Do not blur the two into decorative mixtures inside the same component.

## 4. Elevation

This system uses tonal layering first, shadows secondary. Most depth comes from the distinction between page field, panel surface, and stronger card surface, not from heavy lifted cards. Shadows are soft and ambient, used to help sticky elements, chips, dialogs, and hover states separate from the field without creating SaaS-style stacked-card clutter.

### Shadow Vocabulary
- **Ambient Surface Shadow** (`0 18px 54px rgba(18, 38, 29, 0.08)`): the default panel shadow, used under major surfaces and chips.
- **Sticky Header Lift** (`0 16px 34px rgba(18, 38, 29, 0.08)`): a tighter shadow for the masthead so it can float over the field while remaining quiet.
- **Interactive Lift** (`0 14px 28px rgba(18, 38, 29, 0.2)`): used for the scroll-to-top control and other smaller active elements.
- **Dialog Depth** (`0 32px 90px rgba(0, 0, 0, 0.28)`): reserved for modal evidence presentation where a real separation plane is needed.

### Named Rules
**The Layer-First Rule.** If tonal contrast can create separation, use it before adding a stronger shadow.

## 5. Components

### Buttons
- **Character:** restrained and civic
- **Shape:** fully rounded pills for compact controls (`999px`) and softer rounded rectangles for secondary actions (`12px` to `18px` where needed)
- **Primary:** the system’s most button-like affordances use `Monsoon Desk Green` with light text, medium-to-bold label typography, and generous horizontal padding
- **Hover / Focus:** hover states rely on tonal shift and slight lift, not glow or bounce. Focus should remain high-contrast and non-theatrical.
- **Secondary / Ghost:** transparent or warm-white variants with quiet borders are preferred over heavy outlined enterprise buttons.

### Chips
- **Style:** chips are a core primitive in this UI. Default chips use the accent green with light text; subtle chips invert into translucent warm-white with dark text.
- **State:** chips communicate mode, timestamp, and concise meta-state. They should read as compact utility markers, not decorative tags.

### Cards / Containers
- **Corner Style:** primary panels use `28px`, internal cards use `24px`, callouts and compact detail blocks step down to `18px`, `16px`, `14px`, and `12px`.
- **Background:** panels use `Glass Paper Panel`; cards use `Pressed Paper Card`; the map stage uses a dark layered green field to create contrast against the rest of the page.
- **Shadow Strategy:** ambient shadows support tonal layering but do not dominate it.
- **Border:** borders are low-contrast, paper-ink dividers rather than hard card frames.
- **Internal Padding:** the standard rhythm is `22px` for primary panels and `18px` for nested cards.

### Inputs / Fields
- **Style:** inline controls such as the archive selector sit inside warm translucent pill shells rather than exposed form chrome.
- **Focus:** focus should remain clear but restrained, using contrast and border distinction rather than animated glow.
- **Error / Disabled:** error styling should favor text clarity and muted hazard tinting over loud red fields.

### Navigation
- **Style:** top-level navigation is rendered as pill links inside a floating masthead. The active tab fills with accent green; inactive tabs sit on warm translucent white.
- **Typography:** compact bold sans labels keep navigation direct and scannable.
- **Mobile treatment:** the masthead collapses into a stacked layout below `980px`, preserving clarity over compactness.

### Signature Component
- **Risk cards:** these are the signature reading unit of the system. Each card combines level pill, score, one-line summary, and compact metadata so users can shift between browsing and evidence inspection without losing orientation.

## 6. Do's and Don'ts

### Do:
- **Do** keep the page rooted in warm neutrals like `#efe8d8`, `#f8f5ef`, and `#d9e2d4` instead of drifting toward pure white or cold gray.
- **Do** use `#214f3f` as the primary civic accent for navigation, links, chips, and compact emphasis.
- **Do** let serif typography frame public meaning and let sans typography carry the working UI.
- **Do** preserve the current rounded surface rhythm: `28px` for panels, `24px` for cards, and `999px` for pills.
- **Do** keep transitions short and quiet, around `180ms`, with opacity, transform, or tonal changes rather than layout animation.
- **Do** make risk cues color-blind-safe by pairing color with text labels, pills, and explicit level names.

### Don't:
- **Don't** let the interface feel like a **government PDF portal**: no document-like density walls, no flat beige tables without hierarchy, no bureaucratic framing.
- **Don't** let it become a **generic SaaS dashboard**: no interchangeable KPI tiles, no cold enterprise blues, no “hero metric” cliches.
- **Don't** let it become an **alarmist disaster app**: no panic reds across the page, no flashing urgency, no exaggerated alert styling for weak evidence.
- **Don't** let it become a **newsfeed clone**: this is a reading-and-judgment surface, not a chronology of updates.
- **Don't** replace tonal layering with decorative glassmorphism everywhere. The current blur usage in the masthead and panels is already the upper limit.
- **Don't** use pure black, pure white, or bright un-tinted neutrals that break the paper-field character of the system.
