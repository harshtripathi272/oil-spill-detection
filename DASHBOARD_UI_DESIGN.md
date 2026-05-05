# VesselWatch: Modern Government Oil Spill Detection Dashboard
## Professional UI/UX Design Specification

**Version**: 1.0  
**Date**: May 5, 2026  
**Classification**: Official Use  
**Last Updated**: May 5, 2026

---

## 📋 Executive Summary

VesselWatch is a sophisticated, real-time marine surveillance dashboard designed for government maritime agencies, environmental protection departments, and coast guard operations. The system integrates vessel tracking (AIS), satellite imagery (Sentinel-1 SAR), and AI-powered anomaly detection to identify and monitor potential oil spill incidents.

**Key Dashboard Goals:**
- Provide actionable intelligence to maritime authorities in real-time
- Present complex environmental and vessel data intuitively
- Enable rapid incident assessment and response coordination
- Maintain professional, government-grade visual standards
- Ensure accessibility and usability for 24/7 operations centers

---

## 🎨 Design Philosophy & Visual Identity

### Design Principles

1. **Information Clarity**: Complex data presented in clear, hierarchical visual hierarchy
2. **Government Professionalism**: Modern but authoritative visual language
3. **Operational Efficiency**: Minimize clicks to critical information (2-3 clicks max)
4. **Real-time Responsiveness**: Smooth animations showing data updates
5. **Dark Mode Priority**: Designed for 24/7 operations center environments
6. **Accessibility First**: WCAG 2.1 AA compliance, high contrast ratios
7. **Performance**: Smooth 60 FPS animations, sub-200ms data load times

### Color Palette

#### Primary Colors
- **Primary Blue**: `#0066CC` - Trust, authority, government standard
- **Accent Cyan**: `#00D9FF` - Real-time activity, highlights
- **Success Green**: `#00CC66` - Confirmed incidents, healthy status
- **Warning Orange**: `#FF9900` - Potential concerns, requiring review
- **Critical Red**: `#FF3333` - High-priority incidents, system errors
- **Neutral Dark**: `#0F1419` - Primary background
- **Neutral Light**: `#E8EFF8` - Text on dark backgrounds

#### Status Colors
- **Detected**: `#FF9900` (Warning Orange) - New, unconfirmed
- **Confirmed**: `#FF3333` (Critical Red) - High confidence
- **False Positive**: `#666666` (Gray) - Dismissed
- **Resolved**: `#00CC66` (Success Green) - Incident closed

### Typography

- **Headlines**: Inter Bold, 32-20px (600 weight)
- **Subheadings**: Inter SemiBold, 18-16px (600 weight)
- **Body Text**: Inter Regular, 14-13px (400 weight)
- **Data/Numbers**: IBM Plex Mono, 14-12px (500 weight)
- **Buttons/Labels**: Inter Medium, 14px (500 weight)

### Visual Effects & Animations

- **Page Transitions**: 300ms fade + 200ms slide-up
- **Data Updates**: Smooth color transitions (300ms ease-out)
- **Hover States**: Subtle scale (1.02) + shadow elevation
- **Loading States**: Elegant skeleton screens with pulse animation
- **Map Interactions**: Smooth zoom/pan with easing
- **Chart Animations**: Staggered data visualization entry (800ms total)
- **Scrolling**: Smooth passive scroll listener for parallax effects

---

## 📐 Layout & Navigation Architecture

### Global Navigation Structure

```
┌─────────────────────────────────────────────────────────┐
│  VesselWatch  [Logo]        Dashboard  Map  Incidents   │  ← Header
│                             Alerts  Analytics  Settings │
└─────────────────────────────────────────────────────────┘
│                                                         │
│  ▓ Sidebar  │                                          │
│  ▓          │        Main Content Area                 │
│  ▓ Collapse │                                          │
│  ▓ Button   │                                          │
│             │                                          │
│  ○ Overview │                                          │
│  ○ Map View │                                          │
│  ○ Incidents│                                          │
│  ○ Analytics│                                          │
│  ○ System   │                                          │
│  ○ Reports  │                                          │
│             │                                          │
│  ────────   │                                          │
│  User Settings                                         │
│  Logout     │                                          │
└─────────────────────────────────────────────────────────┘
```

### Responsive Breakpoints

- **Desktop**: 1920px+ (Full layout with expanded sidebar)
- **Tablet**: 1024px-1919px (Collapsible sidebar, stacked charts)
- **Mobile**: <1024px (Bottom navigation, full-width cards)

---

## 🏠 Page 1: Dashboard Overview (Home/Landing)

### Purpose
Central hub for command center operations. First view on login. Real-time KPI snapshot with actionable alerts.

### Layout Structure

```
╔═══════════════════════════════════════════════════════════════╗
║                    DASHBOARD OVERVIEW                         ║
║                                                               ║
║  ┌─────────────┬─────────────┬─────────────┬──────────────┐  ║
║  │ Total       │ Active      │ Success     │ Avg Conf.   │  ║
║  │ Incidents   │ Incidents   │ Rate        │ Score       │  ║
║  │    847      │    12       │   98.2%     │   0.87      │  ║
║  │             │             │             │             │  ║
║  │  ↑ 12% MoM  │ ↓ 3% WoW    │   Healthy   │ ↑ 5% Trend  │  ║
║  └─────────────┴─────────────┴─────────────┴──────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ ⚠️  ACTIVE ALERTS (3)                                   │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ 🔴 HIGH: Incident #INC-20260505-001 | 0.92 confidence  │  ║
║  │    Location: 25.8°N, 80.1°W | 2 hours ago             │  ║
║  │    Status: PENDING_IMAGERY | SAR data requested        │  ║
║  │    [View Details] [Update Status]                      │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ 🟡 MEDIUM: Incident #INC-20260505-002 | 0.74 confidence│  ║
║  │    Location: 26.2°N, 79.5°W | 4 hours ago             │  ║
║  │    Status: DETECTED | Awaiting analyst review         │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ 🟡 MEDIUM: System: Kafka broker latency alert | 145ms   │  ║
║  │    Action: Monitor | Last updated: 15 mins ago        │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌───────────────────────────┬───────────────────────────────┐ ║
║  │  INCIDENTS OVER TIME (30d)│  STATUS DISTRIBUTION          │ ║
║  │                           │                               │ ║
║  │   ▁▁▂▂▃▃▄▄▅▅▆▆▇▇▇▇▆▆▅▅▄▄ │    Detected: 45% (380)        │ ║
║  │                           │    Confirmed: 35% (296)      │ ║
║  │  Days: 1   5   10   15    │    False Pos: 15% (127)      │ ║
║  │       20   25   30        │    Resolved: 5% (42)         │ ║
║  │  Avg: 28.2 incidents/day  │                              │ ║
║  └───────────────────────────┴───────────────────────────────┘ ║
║                                                               ║
║  ┌───────────────────────────┬───────────────────────────────┐ ║
║  │  PROCESSING TIME TREND    │  MODEL PERFORMANCE (F1)       │ ║
║  │                           │                               │ ║
║  │    Avg: 2.4 hours         │  YOLO11 XL-Seg: ████░ 0.92  │ ║
║  │    ↓ 8% improvement       │  UNet (SMP):     ███░░ 0.88  │ ║
║  │    Min: 1.2h   Max: 8.1h  │  Ensemble:       █████ 0.95  │ ║
║  │                           │                              │ ║
║  │    [See Detailed Metrics] │  [Model Comparison]          │ ║
║  └───────────────────────────┴───────────────────────────────┘ ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 🌍 GEOGRAPHIC DISTRIBUTION (5 Ocean Regions)           │  ║
║  │                                                         │  ║
║  │  North Atlantic:     245 incidents (29%)   ▓▓▓▓▓        │  ║
║  │  South Atlantic:     183 incidents (22%)   ▓▓▓▓         │  ║
║  │  Pacific Ocean:      156 incidents (18%)   ▓▓▓          │  ║
║  │  Mediterranean:      142 incidents (17%)   ▓▓▓          │  ║
║  │  Southeast Asia:     121 incidents (14%)   ▓▓           │  ║
║  │                                                         │  ║
║  │  [Drill Down to Regional Details]                      │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 📊 RECENT INCIDENTS (Last 5)                            │  ║
║  ├──────────┬────────┬─────────┬──────────┬──────────────┤  ║
║  │ Incident │ Conf.  │ Location│ Time Ago │ Status      │  ║
║  ├──────────┼────────┼─────────┼──────────┼──────────────┤  ║
║  │ INC-001  │ 0.92   │ 25.8,-80.1│ 2h ago │ PENDING IMG │  ║
║  │ INC-002  │ 0.74   │ 26.2,-79.5│ 4h ago │ DETECTED    │  ║
║  │ INC-003  │ 0.68   │ 24.9,-81.0│ 6h ago │ CONFIRMED   │  ║
║  │ INC-004  │ 0.45   │ 27.1,-78.9│ 12h ago│ FALSE POS   │  ║
║  │ INC-005  │ 0.91   │ 25.3,-80.8│ 14h ago│ RESOLVED    │  ║
║  │                                                         │  ║
║  │ [View All Incidents]  [Download Report]               │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
```

### Components & Interactions

#### 1.1 KPI Card Section (Top)
- **Four metric cards** in responsive grid
- **Each card** displays:
  - Icon (relevant to metric)
  - Current value (large, bold, monospace font)
  - Month-over-month or trend indicator (↑/↓ with percentage)
  - Sparkline micro-chart (optional)
  - Small help tooltip on hover
  
- **Interactions**:
  - Card click → Detailed metric view
  - Hover → Show 24-hour trend sparkline
  - Number hover → Tooltip shows data source and last refresh time

#### 1.2 Active Alerts Panel
- **Three-level alert system**:
  - 🔴 Critical (confidence > 0.85 + pending validation)
  - 🟡 Medium (0.65-0.85 confidence)
  - 🟢 Info (system messages)

- **Each alert card** contains:
  - Color-coded badge + icon
  - Incident/system name
  - Brief description (1-2 lines)
  - Timestamp (relative: "2 hours ago")
  - Action buttons (View Details, Acknowledge, Update Status)
  - Subtle background animation for critical alerts

- **Interactions**:
  - Click card → Route to Incident Detail page
  - "Update Status" → In-line status selector dropdown
  - Dismiss → Alert fades out with 200ms animation
  - New alerts → Slide in from top with notification sound (optional)

#### 1.3 Charts Section (2x2 Grid)
- **Charts update** every 30 seconds via polling
- **Smooth transitions** when new data arrives:
  - Old bars/lines fade out (200ms)
  - New values animate in (400ms with easing)

**Chart 1: Incidents Over Time**
- Line chart showing 30-day trend
- X-axis: Days of month
- Y-axis: Incident count (auto-scaled)
- Multiple series: Total, Detected, Confirmed, Resolved
- Interactive: Hover shows exact counts, click legend to toggle series

**Chart 2: Status Distribution**
- Doughnut/pie chart
- Segments: Detected (45%), Confirmed (35%), False Positive (15%), Resolved (5%)
- Color-coded per status
- Center text shows total count
- Click segment → Filter incidents list by status

**Chart 3: Processing Time Trend**
- Line chart: Average processing time per day
- Shows improvement/regression trend
- Benchmark line (target: 2 hours)
- Hover: Show exact time + processing time breakdown

**Chart 4: Model Performance**
- Horizontal bar chart
- Metrics: YOLO11 XL-Seg, UNet, Ensemble
- F1 scores displayed (0.88-0.95 range)
- Color gradient: Red (low) → Green (high)
- Click bar → Detailed model metrics page

#### 1.4 Geographic Distribution Panel
- Stacked bar chart with 5 ocean regions
- Percentages + absolute counts
- Color code: Darker shade = higher incident density
- Interactive: Click region → Map zooms to that area

#### 1.5 Recent Incidents Table
- 5-row table showing latest incidents
- Columns: Incident ID | Confidence Score | Location (Lat,Lon) | Time Ago | Status
- Row colors reflect status
- Click row → Detail page
- Pagination: "View All Incidents" link at bottom

### Animations & Effects

- **Page Load**: Staggered fade-in of sections (100ms intervals)
- **Data Updates**: 300ms color transition for metric changes
- **Chart Rendering**: Staggered bar/line animations
- **Alert Arrivals**: Slide-down animation with shadow
- **Hover Effects**: Subtle scale (1.02) on cards + shadow elevation

---

## 🗺️ Page 2: Interactive Incident Map

### Purpose
Geospatial visualization of all detected incidents with real-time positioning and SAR validation status.

### Layout Structure

```
╔═══════════════════════════════════════════════════════════════╗
║                    INCIDENT MAP VIEW                          ║
║  [Filter]  [Zoom Controls]  [Legend]  [Fullscreen]  [Export]│
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │                                                         │  ║
║  │                   🗺️ INTERACTIVE MAP                   │  ║
║  │              (Mapbox/Leaflet with GL)                  │  ║
║  │                                                         │  ║
║  │        Satellite View | Hybrid | Heatmap               │  ║
║  │                                                         │  ║
║  │                                                         │  ║
║  │  🔴 (markers with status icons)                        │  ║
║  │       🟡                                               │  ║
║  │           🟢   [Selected Incident Info]               │  ║
║  │                 ID: INC-20260505-001                   │  ║
║  │                 Confidence: 0.92                       │  ║
║  │                 Status: PENDING_IMAGERY                │  ║
║  │                 Location: 25.80°N, 80.10°W            │  ║
║  │                 Detected: 2 hours ago                  │  ║
║  │                 [View Details] [Close]                │  ║
║  │                                                         │  ║
║  │  ──────── (20km ROI circle for selected)              │  ║
║  │                                                         │  ║
║  │                                                         │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 🔍 FILTER & SEARCH                                     │  ║
║  │                                                         │  ║
║  │  Status: [All ▼] Confidence Range: [0.5 ──●────] 1.0  │  ║
║  │  Time Range: [Last 30 days ▼]                          │  ║
║  │  Region: [All Regions ▼] Search: [__________] 🔍      │  ║
║  │                                                         │  ║
║  │  [Reset Filters]                                       │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 📋 LEGEND                                              │  ║
║  │                                                         │  ║
║  │  🔴 Confirmed (High confidence, verified)              │  ║
║  │  🟡 Detected (New, unconfirmed)                       │  ║
║  │  🟢 Resolved (Incident closed)                         │  ║
║  │  ⚪ False Positive (Dismissed)                         │  ║
║  │  ──── 20km ROI Buffer (Around selected)               │  ║
║  │  🌡️  Heatmap (Incident density)                       │  ║
║  │                                                         │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
```

### Features & Interactions

#### 2.1 Base Map Layer
- **Default**: Satellite imagery layer (Mapbox Streets as fallback)
- **Toggle Options**:
  - Satellite (full resolution satellite)
  - Hybrid (satellite + labels)
  - Street (vector tiles)
  - Heatmap (incident density density overlay)

- **Performance**: WebGL rendering for smooth interactions
- **Zoom Levels**: 3 (world) to 18 (specific coordinates)

#### 2.2 Incident Markers
- **Marker Types** by status:
  - 🔴 **Confirmed**: Solid red circle, size 24px, pulsing glow
  - 🟡 **Detected**: Solid orange circle, size 20px
  - 🟢 **Resolved**: Solid green circle, size 20px
  - ⚪ **False Positive**: Gray circle, size 16px, lower opacity

- **Marker Information** (on click):
  - Incident ID
  - Confidence score (with color-coded bar)
  - Location (coordinates)
  - Detection time (relative)
  - Current status
  - Brief description

- **Marker Clustering**: At zoom levels < 8, auto-cluster markers
  - Cluster color indicates density
  - Click cluster → Zoom in automatically

#### 2.3 ROI Visualization
- **20km Buffer Circle**: Appears when marker selected
  - Transparent blue fill
  - Solid cyan border
  - Legend annotation
  - Shows extent of satellite search area

- **SAR Coverage**: Optional overlay showing
  - Sentinel-1 imagery boundaries (if available)
  - Footprint of processed imagery
  - Detection boxes from model inference

#### 2.4 Interactive Controls

**Left Sidebar Controls:**
- Zoom in/out buttons (+ / -)
- Recenter map (compass icon)
- Reset zoom to world view
- Toggle fullscreen (expand map to full viewport)

**Top Filter Bar:**
- **Status Filter**: Dropdown - All, Confirmed, Detected, Resolved, False Positive
- **Confidence Slider**: Range selector 0.5 - 1.0
- **Time Range**: Dropdown - Last 24h, 7d, 30d, 90d, Custom Range
- **Region Filter**: Dropdown - All, North Atlantic, South Atlantic, Pacific, Mediterranean, Southeast Asia
- **Search Box**: Search by Incident ID or vessel MMSI

**Bottom Legend:**
- Color-coded incident types
- ROI buffer explanation
- Heatmap density scale

#### 2.5 Info Window (Contextual Popup)

```
┌──────────────────────────────────┐
│ 🔴 Incident #INC-20260505-001   │
│ ────────────────────────────────│
│ Location: 25.80°N, 80.10°W      │
│ Confidence: ████████░░ 0.92     │
│ Status: PENDING_IMAGERY         │
│ Detection Time: 2 hours ago     │
│ Model Used: YOLO11 XL-Seg       │
│ Processing Time: 2h 14m         │
│ ────────────────────────────────│
│ [View Details]  [Update Status] │
│ [View SAR Image]                │
└──────────────────────────────────┘
```

#### 2.6 Heatmap Layer

- Optional overlay showing incident density
- Color scale: Low (green) → Medium (yellow) → High (red)
- Opacity slider to adjust visibility
- Updates in real-time as new incidents arrive

#### 2.7 Drawing Tools (Optional Advanced)

- Draw polygon → Filter incidents within bounds
- Draw circle → Find all incidents within radius
- Measure distance/area tools
- Export selected ROI as GeoJSON

### Animations & Effects

- **Marker Animation**: Pulsing glow on confirmed incidents (2s cycle)
- **Marker Hover**: Scale to 1.2x, shadow elevation, bounce effect
- **Cluster Animation**: Staggered marker expansion when cluster clicked
- **Info Window**: Fade-in with 200ms duration
- **Pan/Zoom**: Smooth easing (250ms per interaction)
- **Heatmap Update**: Color gradient transition (300ms)

---

## 📊 Page 3: Incident Management & Details

### 3A: Incident List View

#### Layout Structure

```
╔═══════════════════════════════════════════════════════════════╗
║                    INCIDENT MANAGEMENT                        ║
║                                                               ║
║  [Filters]  [Sort ▼]  [Export CSV]  [Refresh]  [New View]   │
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ Status: [All ▼] Conf: [0.5────●────1.0] Time: [30d ▼]  │ ║
║  │ Region: [All ▼] Search: [____________]  [Advanced ▼]    │ ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  Showing 47 incidents (Page 1 of 3)                           ║
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ ☐ │ Incident │ Location   │ Conf  │ Detected  │ Status  │ ║
║  ├──────────────────────────────────────────────────────────┤ ║
║  │ ☑  │ INC-001  │ 25.8,-80.1│ 0.92 │ 2h ago   │ 🔴 PEND │ ║
║  │    │ Vessel:  │ Tug Boat   │ S/N: │ 245,132  │          │ ║
║  │ ☐  │ INC-002  │ 26.2,-79.5│ 0.74 │ 4h ago   │ 🟡 DET  │ ║
║  │    │ Vessel:  │ Cargo Ship │ S/N: │ 246,521  │          │ ║
║  │ ☐  │ INC-003  │ 24.9,-81.0│ 0.68 │ 6h ago   │ 🟢 CONF │ ║
║  │    │ Vessel:  │ Tanker     │ S/N: │ 247,108  │          │ ║
║  │ ☐  │ INC-004  │ 27.1,-78.9│ 0.45 │ 12h ago  │ ⚪ FP   │ ║
║  │    │ Vessel:  │ Fishing    │ S/N: │ 247,654  │          │ ║
║  │ ☐  │ INC-005  │ 25.3,-80.8│ 0.91 │ 14h ago  │ 🟢 RES  │ ║
║  │    │ Vessel:  │ Oil Tanker │ S/N: │ 248,201  │          │ ║
║  │                                                          │ ║
║  │ ... [20 more rows]                                       │ ║
║  │                                                          │ ║
║  │ [< Previous]  Page 1 of 3  [Next >]                     │ ║
║  │ Show: [50 ▼] per page                                   │ ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ✓ 15 incidents selected.  [Bulk Update Status] [Export]    │ ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
```

#### Features

**Filtering & Sorting:**
- Status dropdown (All, Detected, Confirmed, False Positive, Resolved)
- Confidence range slider
- Time range dropdown (24h, 7d, 30d, 90d, Custom)
- Region multi-select dropdown
- Search field (Incident ID, MMSI, Location)
- Advanced filters toggle (Processing time, Model version, etc.)

**Table Columns:**
- Checkbox (select for bulk actions)
- Incident ID (clickable → detail page)
- Location (Lat, Lon) - shows mini map on hover
- Confidence Score (color bar: red → yellow → green)
- Detected Time (relative: "2 hours ago")
- Status (color-coded badge)
- Actions (three-dot menu)

**Row Expansion:**
- Click row → Expand to show:
  - Vessel name & MMSI
  - SAR image status
  - Processing time
  - Model version used
  - Quick action buttons

**Bulk Actions:**
- Select multiple incidents via checkboxes
- Bulk update status
- Bulk export to CSV/JSON
- Bulk assign to analyst

**Column Customization:**
- Show/hide columns via settings
- Rearrange column order
- Save custom view

---

### 3B: Incident Detail View

#### Layout Structure

```
╔═══════════════════════════════════════════════════════════════╗
║                INCIDENT DETAIL: #INC-20260505-001            ║
║                                                               ║
║  [< Back to List]  [Map View]  [Edit]  [Assign]  [Close]    │
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ STATUS: 🔴 PENDING_IMAGERY                              │  ║
║  │ Confidence: ████████░░ 0.92 (Very High)                 │  ║
║  │ Detection Time: May 5, 2026 14:32 UTC (2 hours ago)    │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌───────────────────────────┬───────────────────────────────┐ ║
║  │ INCIDENT INFORMATION      │ LOCATION & COORDINATES       │  ║
║  ├───────────────────────────┼───────────────────────────────┤ ║
║  │ Incident ID:  INC-001     │ Latitude:   25.8015°N        │  ║
║  │ Detection ID: DET-12345   │ Longitude:  80.1024°W        │  ║
║  │ Model Used:   YOLO11 XL   │ Region:     Gulf of Mexico   │  ║
║  │ Model Ver.:   11.0.2      │ ROI Radius: 20 km            │  ║
║  │ Processing Time: 2h 14m   │ Bounding Box:                │  ║
║  │ Processing Status: ⏳      │   NW: 25.85, -80.15         │  ║
║  │ Completed: 16:46 UTC      │   SE: 25.75, -80.05         │  ║
║  │                           │                              │  ║
║  │ Anomaly Scores:           │ [🗺️ View on Map]             │  ║
║  │  Physics:    0.90         │                              │  ║
║  │  Global:     0.88         │                              │  ║
║  │  Local:      0.82         │                              │  ║
║  │  Vessel:     0.85         │                              │  ║
║  └───────────────────────────┴───────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ VESSEL INFORMATION (Source: AIS)                        │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ MMSI: 245132156          │ Type: Oil Tanker              │  ║
║  │ Ship Name: "Pacific Wave" │ Flag: Liberia                │  ║
║  │ Callsign: PFWV            │ Length: 228m                 │  ║
║  │ IMO: 9876543              │ Beam: 32m                    │  ║
║  │ Ship Type: 70 (Cargo)     │ Deadweight: 25,000 tons      │  ║
║  │ Speed (last report): 12.5 knots                         │  ║
║  │ Heading (last report): 245°                             │  ║
║  │ Status: Underway                                        │  ║
║  │ [View AIS Track] [View Vessel Details]                  │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ SATELLITE VALIDATION (SAR/Sentinel-1)                  │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ Search Status: ⏳ IN_PROGRESS                           │  ║
║  │ Search Window: ±24 hours from event time                │  ║
║  │ Search Area: 20 km buffer around coordinates            │  ║
║  │ Products Found: 0 (Still searching)                     │  ║
║  │ Last Update: 2026-05-05 16:30 UTC                       │  ║
║  │                                                         │  ║
║  │ When imagery is available:                             │  ║
║  │ [SAR Image Preview]  [Download Image]  [Full Results]  │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ PROCESSING WORKFLOW (Airflow DAG: suspicious_event_val)│  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ DAG Run ID: suspicious_event_validation_20260505_143200 │  ║
║  │ Status: RUNNING                                         │  ║
║  │ Started: 2026-05-05 14:32 UTC (2h 18m ago)             │  ║
║  │                                                         │  ║
║  │ Timeline:                                               │  ║
║  │ [✓] wait_for_sar_trigger          [1s]    ✓ SUCCESS   │  ║
║  │ [✓] initialize_incident            [2s]    ✓ SUCCESS   │  ║
║  │ [✓] prepare_search_params          [1s]    ✓ SUCCESS   │  ║
║  │ [⏳] sentinel_search               [...]    ⏳ RUNNING  │  ║
║  │ [ ] sentinel_download              [-]     ◯ WAITING   │  ║
║  │ [ ] sar_inference                  [-]     ◯ WAITING   │  ║
║  │ [ ] process_results                [-]     ◯ WAITING   │  ║
║  │                                                         │  ║
║  │ [View Full DAG Visualization]  [Logs]  [Retry]        │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ STATUS UPDATE & ACTIONS                                │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ Current Status: 🔴 PENDING_IMAGERY                      │  ║
║  │ Change to: [Detected ▼]                                │  ║
║  │ Notes: [Awaiting SAR confirmation...       ]           │  ║
║  │                                                         │  ║
║  │ [Submit Status Update]  [Assign to Analyst]            │  ║
║  │ [Add Note]  [Request Manual Review]                    │  ║
║  │ [Escalate as Critical]  [Close Incident]               │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ HISTORICAL NOTES & ACTIVITY LOG                         │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ 14:32 UTC - System: Incident detected by anomaly model  │  ║
║  │ 14:35 UTC - System: Confidence score calculated: 0.92   │  ║
║  │ 14:40 UTC - System: SAR search triggered                │  ║
║  │ 14:42 UTC - John Doe: "High confidence. Likely oil."   │  ║
║  │ 15:10 UTC - System: Awaiting SAR imagery...             │  ║
║  │                                                         │  ║
║  │ [Load More History]                                    │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ EXPORT & PRINT                                          │  ║
║  │ [Export as PDF]  [Export as JSON]  [Print]             │  ║
║  │ [Email Report]   [Share Link]      [Archive]           │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
```

#### Key Sections

**1. Status Banner (Top)**
- Large, color-coded status badge
- Confidence meter (visual bar + percentage)
- Detection time (absolute + relative)
- Quick action buttons

**2. Incident Information Card**
- Incident ID, detection ID, model version
- Processing time and completion timestamp
- Anomaly scores breakdown (physics, global, local, vessel)
- Location coordinates with ROI details

**3. Vessel Information Card**
- MMSI, Ship name, Callsign, IMO
- Vessel type, flag country
- Dimensions and tonnage
- Last known speed and heading
- AIS track visualization link

**4. Satellite Validation Card**
- Search status (In Progress, Complete, Failed)
- Search parameters (time window, area)
- Products found count
- SAR image preview (when available)
  - Thumbnail of SAR imagery
  - Detection bounding boxes overlay
  - Confidence scores per detection
- Download links
- Full inference results

**5. Processing Workflow Card**
- DAG run ID and status
- Timeline of tasks with:
  - Task name
  - Duration
  - Status (Success/Running/Failed/Waiting)
  - Color-coded visual indicator
- Task dependencies visualization
- Retry mechanism for failed tasks
- Full logs access

**6. Status Update Section**
- Dropdown to change status
- Optional notes field
- Action buttons:
  - Submit Status Update
  - Assign to Analyst
  - Add Note
  - Request Manual Review
  - Escalate as Critical
  - Close Incident

**7. Activity Log**
- Chronological list of all changes
- System events (auto-generated)
- User notes/comments
- Timestamps
- Collapsible "Load More History"

---

## 📈 Page 4: Analytics & Reporting

### Layout Structure

```
╔═══════════════════════════════════════════════════════════════╗
║                    ADVANCED ANALYTICS                         ║
║                                                               ║
║  [Date Range: ├─────●────────┤ 30 days]  [Custom Range]     │
║  [Region: All ▼]  [Status Filter: All ▼]  [Model: All ▼]    │
║  [Export: CSV/PDF/JSON]  [Scheduled Reports]                 │
║                                                               ║
║  ╔─────────────────────────────────────────────────────────╗  ║
║  ║ TAB MENU: Overview | Geographic | Temporal | Model     ║  ║
║  ╠─────────────────────────────────────────────────────────╣  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 📍 GEOGRAPHIC ANALYSIS                                  │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │                                                         │  ║
║  │  Region Breakdown:                                      │  ║
║  │  ┌──────────────┬─────────┬──────────┬────────────────┐ │  ║
║  │  │ Region       │ Count   │ % of Tot │ Avg Confidence│ │  ║
║  │  ├──────────────┼─────────┼──────────┼────────────────┤ │  ║
║  │  │ N. Atlantic  │   245   │   29%    │     0.82      │ │  ║
║  │  │ S. Atlantic  │   183   │   22%    │     0.79      │ │  ║
║  │  │ Pacific      │   156   │   18%    │     0.85      │ │  ║
║  │  │ Mediterranean│   142   │   17%    │     0.81      │ │  ║
║  │  │ S.E. Asia    │   121   │   14%    │     0.78      │ │  ║
║  │  └──────────────┴─────────┴──────────┴────────────────┘ │  ║
║  │                                                         │  ║
║  │  [Click region for detailed heatmap]                   │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 📅 TEMPORAL ANALYSIS                                    │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ Incidents by Hour of Day:                               │  ║
║  │ 00:00 ▁ 02:00 ▂ 04:00 ▂ 06:00 ▃ 08:00 ▄ 10:00 ▆ 12:00│  ║
║  │ 14:00 ▇ 16:00 ▅ 18:00 ▄ 20:00 ▂ 22:00 ▁                │  ║
║  │                                                         │  ║
║  │ Day of Week Distribution:                               │  ║
║  │ Mon ▄ Tue ▅ Wed ▄ Thu ▃ Fri ▆ Sat ▇ Sun ▅               │  ║
║  │                                                         │  ║
║  │ Monthly Trend (Last 12 months):                         │  ║
║  │ ▁▂▃▄▅▆▇▇▆▇▆▅ (Shows seasonality)                         │  ║
║  │                                                         │  ║
║  │ Peak Hours: 14:00 - 16:00 UTC (42 incidents)           │  ║
║  │ Lowest Activity: 04:00 - 06:00 UTC (12 incidents)      │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 🎯 MODEL PERFORMANCE COMPARISON                         │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │                                                         │  ║
║  │  Model: YOLO11 XL-Seg     │  Model: UNet (SMP)        │  ║
║  │  ─────────────────────────┼─────────────────────────  │  ║
║  │  Precision:  0.96         │  Precision:  0.91         │  ║
║  │  Recall:     0.88         │  Recall:     0.86         │  ║
║  │  F1 Score:   0.92         │  F1 Score:   0.88         │  ║
║  │  IoU:        0.85         │  IoU:        0.81         │  ║
║  │  Dice:       0.90         │  Dice:       0.87         │  ║
║  │                           │                           │  ║
║  │  Avg Inference Time: 4.2s │  Avg Inference Time: 3.8s │  ║
║  │                                                         │  ║
║  │  Ensemble (Both Models):                                │  ║
║  │  ────────────────────                                   │  ║
║  │  Precision:  0.97   F1 Score:   0.95   IoU: 0.88      │  ║
║  │  Recall:     0.93   Dice:       0.93   Time: 8.0s     │  ║
║  │                                                         │  ║
║  │  [Model Training History] [Switch Active Model]        │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 📊 CUSTOM QUERY BUILDER                                 │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ Build custom report with filters:                       │  ║
║  │                                                         │  ║
║  │ [Confidence > 0.8]  AND  [Region = Atlantic]           │  ║
║  │ AND  [Status = Confirmed]  AND  [Time = Last 7 days]   │  ║
║  │                                                         │  ║
║  │ [+ Add Filter]  [Run Query]  [Save as Report]          │  ║
║  │                                                         │  ║
║  │ Results: 87 incidents matching criteria                │  ║
║  │ [View Chart]  [Download Data]  [Create Alert Rule]    │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
```

#### Key Features

**1. Analytics Tabs**
- **Overview**: Summary statistics and trends
- **Geographic**: Regional breakdowns, heatmaps, density analysis
- **Temporal**: Time-of-day patterns, day-of-week distribution, seasonality
- **Model Performance**: Comparison of YOLO, UNet, Ensemble models

**2. Date Range Selector**
- Visual timeline slider (last 30/90/180 days)
- Custom date range picker
- Preset options (YTD, Last Quarter, etc.)

**3. Regional Analysis**
- 5-region breakdown with incident counts
- Average confidence per region
- Percent of total distribution
- Clickable regions → Drill down to detailed view

**4. Temporal Charts**
- Heatmap: Hour of day vs. Day of week
- Line chart: Monthly trend (12 months)
- Bar chart: Peak hours identification
- Insights: Peak activity time, lowest activity time

**5. Model Comparison**
- Side-by-side metric display:
  - Precision, Recall, F1 Score
  - IoU (Intersection over Union)
  - Dice coefficient
  - Inference time
- Ensemble model aggregate scores
- Model training history chart
- Switch active model for new processing

**6. Custom Query Builder**
- Drag-and-drop filter interface
- Predefined queries (templates)
- Save custom reports
- Schedule automated report generation
- Export results in multiple formats

---

## 🏥 Page 5: System Health & Monitoring

### Layout Structure

```
╔═══════════════════════════════════════════════════════════════╗
║                    SYSTEM HEALTH DASHBOARD                    ║
║                  Last Updated: 2026-05-05 16:45 UTC           ║
║                                                               ║
║  ┌──────────────┬──────────────┬──────────────┬──────────────┐ ║
║  │ 🟢 Overall   │ 🟢 Database  │ 🟢 Services  │ 🟡 Storage   │ ║
║  │ HEALTHY      │ HEALTHY      │ HEALTHY      │ WARNING      │ ║
║  │ Uptime:      │ Connections: │ All Running  │ 78% Used     │ ║
║  │ 47d 12h      │ 18/20 active │ (8 services) │ (98GB/125GB) │ ║
║  └──────────────┴──────────────┴──────────────┴──────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 📊 SYSTEM RESOURCES (Last 1 Hour)                       │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │                                                         │  ║
║  │ CPU Usage:      ████████░░ 62%     (24 cores available) │  ║
║  │ Memory Usage:   ██████░░░░ 48%     (64GB available)    │  ║
║  │ Disk Usage:     ███████░░░ 78%     (125GB total)       │  ║
║  │ Network I/O:    ▂▂▃▃▄▄▅▅▆▆▇▇▆▆▅▅   (Inbound peak: 850M) │  ║
║  │                                                         │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 🗄️  DATABASE STATUS                                     │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ PostgreSQL:             🟢 HEALTHY                      │  ║
║  │   Active Connections:   18 / 20 max                     │  ║
║  │   Query Performance:    Avg 34ms (95th: 120ms)         │  ║
║  │   Replication Lag:      0.2 seconds                     │  ║
║  │   Disk Space Used:      45 GB                           │  ║
║  │   Last Backup:          2026-05-05 15:00 UTC (1h ago)  │  ║
║  │                                                         │  ║
║  │ Redis (Cache):          🟢 HEALTHY                      │  ║
║  │   Memory Used:          4.2 / 8.0 GB                   │  ║
║  │   Connected Clients:    12                              │  ║
║  │   Hit Rate:             87% (Excellent)                │  ║
║  │   Evictions:            0 (No pressure)                │  ║
║  │                                                         │  ║
║  │ Supabase API:           🟢 HEALTHY                      │  ║
║  │   Response Time:        Avg 145ms                       │  ║
║  │   Last Sync:            2026-05-05 16:41 UTC (4m ago)  │  ║
║  │                                                         │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 🚀 BACKEND SERVICES                                     │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │                                                         │  ║
║  │ FastAPI (Main API Server)      🟢 RUNNING              │  ║
║  │   Uptime: 18d 4h   CPU: 2.1%   Memory: 245MB          │  ║
║  │   Last Restart: 2026-04-17 08:23 UTC                  │  ║
║  │   API Endpoints: 47 / 47 responding                    │  ║
║  │                                                         │  ║
║  │ AIS Ingestion (WebSocket)      🟢 RUNNING              │  ║
║  │   Uptime: 12d 16h  CPU: 3.4%   Memory: 512MB          │  ║
║  │   Connections: 1 active  Messages/sec: 1240            │  ║
║  │   Dead Letter Queue: 2 msgs (0.02% error rate)        │  ║
║  │                                                         │  ║
║  │ Stream Processor (Kafka)       🟢 RUNNING              │  ║
║  │   Uptime: 12d 18h  CPU: 4.2%   Memory: 1.1GB          │  ║
║  │   Messages Processed: 21,247 (99.97% success)         │  ║
║  │   Redis State Entries: 1,842 active vessels            │  ║
║  │                                                         │  ║
║  │ Anomaly Detector (ML)          🟢 RUNNING              │  ║
║  │   Uptime: 8d 3h    CPU: 18.5%  Memory: 2.8GB          │  ║
║  │   Model Loaded: AIS-Contrastive-Encoder-v1            │  ║
║  │   Inference Time: Avg 45ms  Batch Size: 32            │  ║
║  │   Anomalies Detected (24h): 847                        │  ║
║  │                                                         │  ║
║  │ Trigger Bridge                 🟢 RUNNING              │  ║
║  │   Uptime: 10d 5h   CPU: 1.2%   Memory: 180MB          │  ║
║  │   Trigger Events Sent: 247                             │  ║
║  │   Filter Efficiency: 71% filtered (below threshold)    │  ║
║  │                                                         │  ║
║  │ Airflow Scheduler               🟢 RUNNING             │  ║
║  │   Uptime: 45d 2h   CPU: 2.8%   Memory: 890MB          │  ║
║  │   DAGs Deployed: 1  (suspicious_event_validation)     │  ║
║  │   Active DAG Runs: 3                                   │  ║
║  │   Last DAG Run Success Rate: 98.4%                     │  ║
║  │                                                         │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 📡 KAFKA & MESSAGE BROKER                               │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │                                                         │  ║
║  │ Broker Status:   🟢 HEALTHY                            │  ║
║  │   Brokers: 3/3 online                                  │  ║
║  │   Topics: 5                                            │  ║
║  │                                                         │  ║
║  │ Topic: ais.raw.position_reports                        │  ║
║  │   Partitions: 8  Replicas: 2  Consumer Lag: 0         │  ║
║  │   Messages/sec: 12.4  Size: 28GB                      │  ║
║  │                                                         │  ║
║  │ Topic: ais.cleaned.position_reports                    │  ║
║  │   Partitions: 4  Replicas: 2  Consumer Lag: 0         │  ║
║  │   Messages/sec: 11.8  Size: 15GB                      │  ║
║  │                                                         │  ║
║  │ Topic: ais.features.vessel_tracks                      │  ║
║  │   Partitions: 4  Replicas: 2  Consumer Lag: 0         │  ║
║  │   Messages/sec: 10.2  Size: 8.2GB                     │  ║
║  │                                                         │  ║
║  │ Topic: ais.anomalies.events                            │  ║
║  │   Partitions: 2  Replicas: 2  Consumer Lag: 0         │  ║
║  │   Messages/sec: 2.1   Size: 3.4GB                     │  ║
║  │                                                         │  ║
║  │ Topic: sar.trigger.events                              │  ║
║  │   Partitions: 2  Replicas: 2  Consumer Lag: 0         │  ║
║  │   Messages/sec: 0.8   Size: 1.2GB                     │  ║
║  │                                                         │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ ⚠️  ALERTS & NOTIFICATIONS (Last 24 Hours)              │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ 🟢 2026-05-05 14:30: Storage usage at 78% (Warning)   │  ║
║  │ 🟢 2026-05-05 12:15: Query latency spike resolved      │  ║
║  │ 🟢 2026-05-04 18:42: Routine backup completed          │  ║
║  │ 🟢 2026-05-04 08:00: Daily health check passed         │  ║
║  │ 🟢 2026-05-03 22:30: Kafka consumer lag normalized     │  ║
║  │                                                         │  ║
║  │ [View Alert History]  [Configure Alert Rules]          │  ║
║  │ [Suppress Notifications]  [Export Log]                 │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
║  ┌─────────────────────────────────────────────────────────┐  ║
║  │ 🔧 MAINTENANCE & OPERATIONS                             │  ║
║  ├─────────────────────────────────────────────────────────┤  ║
║  │ [Backup Now]  [Export Database]  [Clear Cache]         │  ║
║  │ [Restart Services]  [View Logs]  [Contact Support]     │  ║
║  │ [Scheduled Maintenance: 2026-05-12 02:00 UTC (8 hrs)]  │  ║
║  └─────────────────────────────────────────────────────────┘  ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
```

#### Key Features

**1. System Status Overview**
- 4-card grid showing overall system status
- Component status indicators (Healthy/Warning/Critical)
- Key metrics per component
- Color-coded badges

**2. Resource Monitoring**
- Real-time CPU, Memory, Disk, Network metrics
- Trend graphs (last 1 hour)
- Threshold indicators
- Warning states when approaching limits

**3. Database Status**
- PostgreSQL health:
  - Active connections / max pool
  - Query performance metrics
  - Replication lag
  - Disk space usage
  - Last backup timestamp
- Redis cache health:
  - Memory utilization
  - Connected clients
  - Cache hit rate
  - Evictions counter
- Supabase integration status

**4. Service Health Dashboard**
- List of 6 key backend services:
  - FastAPI API Server
  - AIS Ingestion
  - Stream Processor
  - Anomaly Detector
  - Trigger Bridge
  - Airflow Scheduler

- Per-service info:
  - Status indicator (Running/Stopped/Warning)
  - Uptime counter
  - CPU & Memory usage
  - Key metrics (messages/sec, inference time, etc.)
  - Last restart timestamp

**5. Kafka Broker Status**
- Broker health (all online)
- Topic breakdown:
  - Partition count
  - Replication factor
  - Consumer lag
  - Message throughput
  - Disk space per topic

**6. Alert Log**
- Chronological list of system events (last 24 hours)
- Alert severity levels
- Resolved alerts marked with checkmark
- View alert history, configure rules
- Suppress notifications option

**7. Maintenance Tools**
- Quick action buttons:
  - Trigger backup
  - Export database
  - Clear cache
  - Restart services
- Scheduled maintenance window display
- Contact support link

---

## 🔐 Page 6: Settings & Administration

### Layout Structure

```
╔═══════════════════════════════════════════════════════════════╗
║                     DASHBOARD SETTINGS                        ║
║                                                               ║
║  [Settings]  [Users]  [Roles]  [API Keys]  [Logs]            │
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ 👤 USER PROFILE                                          │ ║
║  ├──────────────────────────────────────────────────────────┤ ║
║  │ Name:              Dr. John Anderson                     │ ║
║  │ Email:             john.anderson@maritime.gov            │ ║
║  │ Organization:      U.S. Coast Guard NOAA                │ ║
║  │ Role:              Administrator                         │ ║
║  │ Department:        Marine Surveillance                   │ ║
║  │ Last Login:        2026-05-05 16:30 UTC                 │ ║
║  │                                                          │ ║
║  │ [Edit Profile]  [Change Password]  [Two-Factor Auth]   │ ║
║  │ [Logout]  [Delete Account]                              │ ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ 🎨 DASHBOARD PREFERENCES                                │ ║
║  ├──────────────────────────────────────────────────────────┤ ║
║  │ Theme:                [Dark ▼]  (Light / System)        │ ║
║  │ Sidebar Behavior:     [Auto-collapse ▼]                 │ ║
║  │ Chart Animation:      [✓] Enabled                       │ ║
║  │ Real-time Updates:    [✓] Enabled  Interval: [30 sec ▼]│ ║
║  │ Default View:         [Overview ▼]                      │ ║
║  │ Rows per Page:        [50 ▼]                            │ ║
║  │ Time Zone:            [UTC ▼]                           │ ║
║  │ Date Format:          [ISO 8601 ▼]                      │ ║
║  │ Language:             [English (US) ▼]                  │ ║
║  │ Accessibility:        [High Contrast] [Dyslexia Font]   │ ║
║  │                                                          │ ║
║  │ [Reset to Defaults]  [Save Preferences]                 │ ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ 🔔 NOTIFICATION SETTINGS                                │ ║
║  ├──────────────────────────────────────────────────────────┤ ║
║  │ Email Notifications:                                     │ ║
║  │   [✓] New High-Confidence Incident                      │ ║
║  │   [✓] Status Updates                                    │ ║
║  │   [✓] System Alerts                                     │ ║
║  │   [ ] Daily Summary Report                              │ ║
║  │                                                          │ ║
║  │ In-App Notifications:                                    │ ║
║  │   [✓] Desktop Alerts                                    │ ║
║  │   [✓] Sound Notifications (on critical)                 │ ║
║  │   [✓] Status LED (left side)                            │ ║
║  │   [✓] Toast Messages                                    │ ║
║  │                                                          │ ║
║  │ Notification Frequency:                                  │ ║
║  │   [Immediate ▼]  (Immediate / Batched / Digest)        │  ║
║  │                                                          │ ║
║  │ Quiet Hours: [Enabled]  From [22:00] To [07:00] UTC   │ ║
║  │ (No notifications except critical during quiet hours)   │ ║
║  │                                                          │ ║
║  │ [Save Preferences]                                      │ ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ 🔑 APIKEYS & INTEGRATIONS                               │ ║
║  ├──────────────────────────────────────────────────────────┤ ║
║  │ Personal API Key:     vesselwatch_sk_****82fe          │ ║
║  │ Created:              2026-01-15                         │ ║
║  │ Last Used:            2026-05-05 16:30 UTC              │ ║
║  │ Scopes:               incidents:read, metrics:read       │ ║
║  │                                                          │ ║
║  │ [Regenerate Key]  [View Usage]  [Revoke]                │ ║
║  │                                                          │ ║
║  │ [Create New API Key]                                    │ ║
║  │ [Manage Integrations]  (Slack, Email, Webhook)         │ ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ 👥 USER MANAGEMENT (Admin Only)                         │ ║
║  ├──────────────────────────────────────────────────────────┤ ║
║  │ Total Users: 24                                          │ ║
║  │ Active (Last 30d): 18                                    │ ║
║  │                                                          │ ║
║  │ [+ Invite New User]  [View All Users]  [Audit Log]      │ ║
║  │                                                          │ ║
║  │ Recent Users:                                            │ ║
║  │ • John Anderson (Admin) - Last: 16:30 UTC              │ ║
║  │ • Sarah Miller (Analyst) - Last: 15:45 UTC             │ ║
║  │ • Michael Chen (Operator) - Last: 14:20 UTC            │ ║
║  │ • Emily Rodriguez (Viewer) - Last: 2d ago              │ ║
║  │                                                          │ ║
║  │ [Edit Roles]  [View Permissions Matrix]                │  ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ 📋 SCHEDULED REPORTS                                    │ ║
║  ├──────────────────────────────────────────────────────────┤ ║
║  │ • Daily Summary (7:00 AM UTC) ─→ john.anderson@...      │ ║
║  │ • Weekly Analytics (Monday 9:00 AM UTC)                 │ ║
║  │ • Monthly Executive Report (1st day at 10:00 AM UTC)   │ ║
║  │                                                          │ ║
║  │ [+ Create New Report]  [Edit Report]  [Delete]          │ ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
║  ┌──────────────────────────────────────────────────────────┐ ║
║  │ 🔐 SECURITY & COMPLIANCE                                │ ║
║  ├──────────────────────────────────────────────────────────┤ ║
║  │ Two-Factor Authentication:   [Enabled ✓]               │ ║
║  │ Session Timeout:             [30 minutes ▼]            │ ║
║  │ IP Whitelist:                [Not configured]           │ ║
║  │ Active Sessions:             2 (Manage)                 │ ║
║  │                                                          │ ║
║  │ [Enable MFA]  [View Session Log]  [Activity Log]        │ ║
║  │ [Configure IP Restrictions]  [Audit Trail]              │ ║
║  └──────────────────────────────────────────────────────────┘ ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
```

---

## 🎬 User Workflows & Navigation Flows

### Workflow 1: Detect and Validate New Oil Spill (5-10 minutes)

```
1. Open Dashboard
   ↓
2. See New Alert (High Confidence Incident)
   ↓
3. Click Alert → View Incident Detail Page
   ↓
4. Review:
   - AIS vessel info
   - Anomaly scores
   - Location
   ↓
5. Click "Map View" to inspect ROI on map
   ↓
6. Zoom in to see 20km buffer around incident
   ↓
7. Return to Detail View
   ↓
8. Wait for SAR Imagery (view DAG progress)
   ↓
9. When available: Review SAR image preview
   ↓
10. Click "View SAR Image" to see inference results
   ↓
11. Update Status: "Confirmed" or "False Positive"
   ↓
12. Add Notes and Assign to Regional Team
   ↓
13. System notifies assigned team
```

### Workflow 2: Generate Weekly Report (3-5 minutes)

```
1. Navigate to Analytics page
   ↓
2. Set Date Range: Last 7 Days
   ↓
3. Select Region Filter: All Regions
   ↓
4. View Summary KPIs, Charts
   ↓
5. Adjust Custom Query (if needed)
   ↓
6. Click "Export as PDF"
   ↓
7. Select Recipients: coast.guard@noaa.gov
   ↓
8. Schedule: Immediate or Scheduled
   ↓
9. Confirmation email sent to recipients
```

### Workflow 3: Monitor System Health (2-3 minutes)

```
1. Click "System Health" in sidebar
   ↓
2. Scan Component Status Cards
   ↓
3. If Warning/Critical: Click to expand details
   ↓
4. Review Metrics:
   - Resource Usage (CPU/Memory/Disk)
   - Service Status
   - Database Health
   ↓
5. Check Alert Log for recent events
   ↓
6. If needed: Trigger manual backup or restart service
   ↓
7. Return to Dashboard
```

---

## 📱 Responsive Design Considerations

### Mobile (< 768px)
- **Layout**: Single column, bottom navigation
- **Map**: Full-width, simplified controls
- **Tables**: Horizontal scroll or card view
- **Charts**: Stacked vertically, reduced detail
- **Sidebar**: Drawer menu (hamburger button)

### Tablet (768px - 1024px)
- **Layout**: Two-column flexible
- **Charts**: 2x2 grid, responsive sizing
- **Map**: Medium zoom limit
- **Sidebar**: Collapsible, takes 25% width
- **Touch targets**: Minimum 44x44px

### Desktop (> 1024px)
- **Full layout** with expanded sidebar
- **Optimized chart layouts**
- **Full data tables with pagination**
- **Smooth scrolling effects**

---

## 🎨 Advanced UI Components

### 1. Status Badge Component

```
Detected:     🟡 DETECTED       (Orange background, 12px padding)
Confirmed:    🔴 CONFIRMED     (Red background)
False Positive: ⚪ FALSE_POSITIVE (Gray background)
Resolved:     🟢 RESOLVED      (Green background)

Animated: Pulse every 2s on confirmed/critical incidents
Tooltip on hover: Show full status name + timestamp
```

### 2. Confidence Meter

```
████████░░ 0.92  (Visual bar + number + label)
Color gradient: Red (0.5) → Yellow (0.7) → Green (0.9+)
Animated: Fill animation on first load (500ms)
```

### 3. Real-time Data Update Indicator

```
🔴 Live updates    🟡 Updating...    🟢 Synced (2 secs ago)
Tooltip: Show data age in seconds
```

### 4. Data Loading Skeleton

```
Placeholder cards with pulse animation
Shows structure while data loads
Reduces perceived load time
```

### 5. Map Marker Tooltip

```
Tooltip on hover (200ms delay):
┌─────────────────────┐
│ INC-20260505-001    │
│ 0.92 confidence     │
│ Gulf of Mexico      │
│ 2 hours ago         │
└─────────────────────┘

Click to open info window
```

---

## 🚀 Performance & Technical Requirements

### Frontend Stack
- **Framework**: React 18+ with TypeScript
- **State Management**: Redux Toolkit or TanStack Query
- **Charting**: Chart.js or Apache ECharts (for high performance)
- **Mapping**: Mapbox GL JS or Leaflet.js
- **Styling**: Tailwind CSS + custom design tokens
- **Animations**: Framer Motion or CSS animations
- **API Calls**: Axios or Fetch API with interceptors

### Performance Targets
- **First Contentful Paint**: < 1.5s
- **Time to Interactive**: < 2.5s
- **Chart Render Time**: < 800ms
- **API Response Time**: < 200ms (at p95)
- **Smooth Scrolling**: 60 FPS minimum
- **Map Interactions**: < 100ms latency

### Backend API Requirements
- RESTful endpoints (already specified in codebase docs)
- WebSocket optional for real-time updates
- JWT authentication
- Rate limiting: 1000 req/min per user
- CORS configuration for frontend domain
- Gzip compression for responses

---

## 📊 Dashboard Summary

| Component | Purpose | Update Frequency | Data Source |
|-----------|---------|------------------|-------------|
| KPI Cards | Top-level metrics snapshot | Every 30 seconds | PostgreSQL aggregate query |
| Alert Panel | Critical incident notifications | Real-time (WebSocket) | Incident table + stream |
| Incident Charts | Trend analysis (30-day) | Every 30 seconds | Metrics table |
| Status Chart | Distribution overview | Every 30 seconds | Incident table query |
| Processing Time | Performance tracking | Every 30 seconds | Metrics table |
| Model Performance | ML validation metrics | Every 60 seconds | Metrics table |
| Geographic Distribution | Regional breakdown | Every 60 seconds | Incident table |
| Map View | Spatial visualization | Real-time | Incident stream |
| Incident List | Detailed incident records | Polling @ 30s | Incident table |
| System Health | Infrastructure status | Every 10 seconds | System monitor service |
| Analytics | Advanced reporting | On-demand | Aggregated queries |

---

## ✅ Implementation Checklist

- [ ] **Authentication UI**
  - [ ] Login page (email/password + MFA)
  - [ ] Password reset flow
  - [ ] Session management

- [ ] **Dashboard Home Page**
  - [ ] KPI cards with real-time updates
  - [ ] Active alerts panel
  - [ ] 4 key charts with animations
  - [ ] Geographic distribution
  - [ ] Recent incidents table
  - [ ] Real-time data polling

- [ ] **Map Page**
  - [ ] Mapbox/Leaflet integration
  - [ ] Incident markers (color-coded)
  - [ ] Click to view incident info
  - [ ] 20km ROI circles
  - [ ] Filter controls
  - [ ] Legend
  - [ ] Heatmap layer

- [ ] **Incident List & Detail**
  - [ ] Filterable/sortable table
  - [ ] Pagination
  - [ ] Detail page with all metadata
  - [ ] SAR image preview section
  - [ ] DAG progress visualization
  - [ ] Status update form
  - [ ] Activity/notes log

- [ ] **Analytics Page**
  - [ ] Date range selector
  - [ ] 4 analysis tabs (Overview, Geographic, Temporal, Model)
  - [ ] Regional breakdown
  - [ ] Time-of-day heatmap
  - [ ] Model comparison metrics
  - [ ] Custom query builder
  - [ ] Export functionality

- [ ] **System Health Page**
  - [ ] Component status cards
  - [ ] Resource monitoring charts
  - [ ] Database health indicators
  - [ ] Service status list
  - [ ] Kafka broker status
  - [ ] Alert log
  - [ ] Maintenance tools

- [ ] **Settings Page**
  - [ ] User profile management
  - [ ] Preferences (theme, update frequency, etc.)
  - [ ] Notification settings
  - [ ] API key management
  - [ ] User management (admin)
  - [ ] Scheduled reports
  - [ ] Security settings

- [ ] **General Features**
  - [ ] Smooth page transitions (fade + slide)
  - [ ] Responsive design (mobile/tablet/desktop)
  - [ ] Dark mode as default
  - [ ] Loading states with skeleton screens
  - [ ] Error handling with user-friendly messages
  - [ ] Empty state views
  - [ ] Accessibility (WCAG 2.1 AA)
  - [ ] Real-time data updates
  - [ ] Export functionality (CSV, PDF, JSON)
  - [ ] Search and filtering across pages

---

## 📝 Design Notes

### Color Psychology
- **Blue**: Authority, trust (government agencies recognize this)
- **Cyan**: Energy, real-time activity, modern tech
- **Red**: Urgent action needed
- **Green**: Safety, resolved status
- **Orange**: Caution, needs review
- **Gray**: Neutral, inactive, false positives

### Typography Hierarchy
- Headlines draw attention to main sections
- Monospace for numerical data (looks precise, technical)
- Consistent sizing creates visual rhythm
- Sufficient line-height (1.6) for readability in operations centers

### Interactive Patterns
- Hover states are subtle (scale + shadow, not aggressive)
- Click targets are at least 44x44px (mobile-friendly)
- Animations are purposeful (feedback, not decoration)
- Loading states reassure users something is happening
- Feedback is immediate (< 100ms)

### Accessibility First
- Minimum 4.5:1 color contrast ratio
- Alt text for all icons
- Keyboard navigation throughout
- Screen reader support for tables/charts
- ARIA labels for dynamic content
- Dyslexia-friendly font option
- High contrast mode

---

**Document Version**: 1.0  
**Last Updated**: May 5, 2026  
**For**: VesselWatch Oil Spill Detection System  
**Classification**: Official Use - Government Maritime Agencies
