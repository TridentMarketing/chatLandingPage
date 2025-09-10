# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a chat support landing page for Travel Resorts of America, built as a static website with modern responsive design. The site serves as a customer support portal with live chat functionality and business hours awareness.

## Architecture

### Core Technologies
- **Frontend Framework**: Pure HTML/CSS/JavaScript with Tailwind CSS via CDN
- **Styling**: Tailwind CSS + custom CSS (hybrid approach)
- **Chat Integration**: SocialIntents chat widget (third-party service)
- **Deployment**: GitHub Pages (hosted at `chat.travelresorts.com`)

### File Structure
- `index.html` - Main landing page with Tailwind CSS integration
- `styles.css` - Legacy CSS (partially used alongside Tailwind)
- `script.js` - Interactive functionality and chat logic
- `full-logo.webp` - Company logo image
- `CNAME` - GitHub Pages custom domain configuration

### Design System
- **Primary Colors**: `#1e3c72` (primary-dark) and `#2a5298` (primary)
- **Accent Color**: `#ffd700` (gold)
- **Gradients**: `linear-gradient(135deg, #1e3c72 0%, #2a5298 100%)`
- **Responsive Breakpoints**: Tailwind defaults (sm: 640px, md: 768px, lg: 1024px)

## Development Commands

### Local Development
```bash
# Start local development server
python3 -m http.server 8000

# Access at: http://localhost:8000
```

### Deployment
```bash
# Deploy to GitHub Pages
git add .
git commit -m "your commit message"
git push origin master
```

## Key Features

### Time-Aware Chat System
- **Business Hours**: Monday-Friday, 9:00 AM - 5:00 PM EST
- **Dynamic Status**: Automatically updates chat availability based on current EST time
- **Fallback**: Email integration for offline hours

### Responsive Design
- **Mobile-First**: Tailwind utilities for responsive design
- **Dynamic Text Sizing**: Scales appropriately across screen sizes
- **Header Management**: Fixed header with dynamic spacing to prevent content overlap

### Chat Integration
- **Primary**: SocialIntents widget (`socialintents.1.4.js`)
- **Fallback**: Custom modal with email redirection
- **Status Indicator**: Real-time online/offline status updates

## Important Implementation Details

### Header Overlap Solution
The site uses dynamic padding-top on the body element to prevent fixed header overlap:
- Desktop: `pt-24 md:pt-32` (96px-128px)
- Mobile: Responsive padding via Tailwind classes

### Color Scheme Consistency
When making changes, maintain the original blue gradient theme:
- Header: White background for logo visibility
- Hero/Footer: Blue gradient (`#1e3c72` to `#2a5298`)
- Accent links: Gold (`#ffd700`)

### Chat Functionality
The `script.js` contains business logic for:
- EST time zone detection
- Business hours validation
- Modal chat interface
- SocialIntents integration
- Smooth scrolling and animations

## Configuration

### Custom Domain
- Domain: `chat.travelresorts.com`
- Configured via `CNAME` file for GitHub Pages

### Third-Party Services
- **SocialIntents**: Chat widget with ID `2c9fa6c37567bb9901756f0407a911b1`
- **Google Fonts**: Poppins font family
- **Tailwind CSS**: Loaded via CDN with custom configuration

## Data Pipeline

### Databricks Integration
A comprehensive notebook (`databricks_socialintents_notebook.py`) pulls chat data from SocialIntents API:

**Key Features:**
- **Configurable modes**: Full historical load vs. daily incremental
- **Date range**: 2023-present with yesterday-only incremental updates  
- **Table**: Single `silver.dechats` table with nested messages array
- **Duplicate prevention**: DELETE before INSERT for data consistency
- **Partitioning**: By `chat_date` for optimal performance

**Configuration:**
```python
INCREMENTAL_MODE = False  # Full load: 2023 to yesterday
INCREMENTAL_MODE = True   # Daily: yesterday only
```

**Credentials Management:**
- API credentials stored in `.env` file (gitignored)
- Databricks secrets recommended: `dbutils.secrets.get(scope="socialintents", key="account_id")`
- Account ID: `2c9fa36b72b45bda0172c33055963258`

**Table Schema:**
- Chat metadata: visitor, agent, duration, timestamps
- Messages: Nested array with type, nickname, timestamp, body
- Processing metadata: ingestion_timestamp, chat_date, message_count

## Maintenance Notes

### CSS Architecture
The project uses a hybrid approach:
- **Tailwind**: For layout, spacing, and responsive design
- **Custom CSS**: Legacy styles in `styles.css` (consider migrating to Tailwind)

### Responsive Considerations
- Hero banner uses `min-h-screen` on mobile for full impact
- Text scales: `text-2xl sm:text-3xl md:text-4xl lg:text-5xl`
- Logo responsive sizing: `h-12 md:h-16`

### Business Logic
- EST time calculations for chat availability
- Email fallbacks for offline hours
- Real-time status updates every minute