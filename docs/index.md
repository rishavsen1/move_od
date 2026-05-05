---
layout: default
title: Home
---

# MOVE-OD

## Modern Origin-Destination Data Generation Platform

MOVE-OD is a comprehensive, web-based tool for generating high-quality origin-destination (OD) point-to-point data from multiple authoritative sources.

<div class="feature-grid">
  <div class="feature">
    <h3>🌐 Web Interface</h3>
    <p>Modern, responsive web application with real-time progress tracking</p>
  </div>
  <div class="feature">
    <h3>📊 Multi-Source</h3>
    <p>Integrates LODES, SafeGraph, MS Buildings, OSM, and INRIX data</p>
  </div>
  <div class="feature">
    <h3>🗺️ Interactive Maps</h3>
    <p>Visualize origins, destinations, and density heatmaps</p>
  </div>
  <div class="feature">
    <h3>📈 Calibrated Output</h3>
    <p>Data calibrated against real-world traffic speeds</p>
  </div>
</div>

---

## Quick Start

### Installation

```bash
git clone https://github.com/yourusername/move_od.git
cd move_od
pip install -r requirements.txt
pip install -r backend/requirements.txt
```

### Start the Application

```bash
# Linux/Mac
./start_webapp_fixed.sh

# Windows
start_webapp.bat

# Docker
docker-compose up -d
```

### Access the Application

Open your browser and navigate to: **http://localhost:8080**

---

## Key Features

### 🎯 Comprehensive Data Integration

- **LODES**: Census employment flow data
- **SafeGraph**: Points of interest and building footprints
- **Microsoft Buildings**: High-precision building polygons
- **OpenStreetMap**: Detailed geographic data
- **INRIX**: Traffic speed data for calibration

### 🖥️ Modern Web Interface

- Intuitive configuration forms
- Real-time progress updates
- Interactive Leaflet maps
- One-click result downloads

### 🗺️ Advanced Visualization

- **Origins & Destinations**: View residential and workplace locations
- **Heatmaps**: Density visualization with color gradients
- **Filtering**: Adjust sample sizes and data types
- **Interactive**: Pan, zoom, and click for details

### 📦 Multiple Output Formats

- GeoJSON for GIS applications
- CSV for data analysis
- ZIP archives for easy sharing
- Metadata for reproducibility

---

## How It Works

<div class="workflow">
  <div class="step">
    <div class="step-number">1</div>
    <h3>Select Location</h3>
    <p>Choose your state and county of interest</p>
  </div>
  
  <div class="step">
    <div class="step-number">2</div>
    <h3>Configure</h3>
    <p>Set date ranges, data sources, and options</p>
  </div>
  
  <div class="step">
    <div class="step-number">3</div>
    <h3>Process</h3>
    <p>Watch real-time progress as data is generated</p>
  </div>
  
  <div class="step">
    <div class="step-number">4</div>
    <h3>Visualize</h3>
    <p>Explore results on interactive maps</p>
  </div>
  
  <div class="step">
    <div class="step-number">5</div>
    <h3>Download</h3>
    <p>Export all results as a ZIP file</p>
  </div>
</div>

---

## Screenshots

### Configuration Interface

![Configuration](images/config-screen.png)
_Easy-to-use configuration form with smart defaults_

### Processing Progress

![Progress](images/progress-screen.png)
_Real-time updates with detailed status messages_

### Interactive Map

![Map](images/map-screen.png)
_Visualize origins (green) and destinations (red)_

### Heatmap View

![Heatmap](images/heatmap-screen.png)
_Density visualization with color gradients_

---

## Use Cases

### 🚗 Transportation Planning

Generate OD data for traffic simulation and analysis:

- SUMO traffic simulation
- Agent-based modeling
- Congestion analysis
- Route optimization

### 🏙️ Urban Planning

Understand commute patterns and accessibility:

- Job-housing balance studies
- Transit planning
- Land use analysis
- Equity assessments

### 📊 Research

Academic and policy research applications:

- Transportation behavior studies
- Economic geography
- Urban analytics
- Public health research

---

## Documentation

<div class="doc-links">
  <a href="USER_GUIDE.html" class="doc-link">
    <h3>📖 User Guide</h3>
    <p>Complete usage documentation</p>
  </a>
  
  <a href="../QUICKSTART.html" class="doc-link">
    <h3>⚡ Quick Start</h3>
    <p>Get up and running fast</p>
  </a>
  
  <a href="../WEB_APP_README.html" class="doc-link">
    <h3>🔧 Technical Guide</h3>
    <p>Architecture and API details</p>
  </a>
  
  <a href="../TROUBLESHOOTING.html" class="doc-link">
    <h3>🔍 Troubleshooting</h3>
    <p>Common issues and solutions</p>
  </a>
</div>

---

## System Requirements

### Minimum Requirements

- **OS**: Linux, macOS, or Windows 10+
- **Python**: 3.8 or higher
- **RAM**: 8 GB
- **Disk Space**: 10 GB free
- **Browser**: Chrome, Firefox, Edge, or Safari

### Recommended Requirements

- **RAM**: 16 GB or more
- **CPU**: 4+ cores
- **Disk Space**: 50 GB free (for multiple counties)
- **Internet**: Stable connection for data downloads

---

## API Reference

### Quick API Overview

```bash
# Get available states and counties
curl http://localhost:8000/api/states

# Start processing
curl -X POST http://localhost:8000/api/process \
  -H "Content-Type: application/json" \
  -d '{"state": "Tennessee", "county": "Hamilton County", ...}'

# Check job status
curl http://localhost:8000/api/job/{job_id}

# Get map data
curl http://localhost:8000/api/map-data/{job_id}?data_type=both&sample_size=1000

# Download results
curl http://localhost:8000/api/download/{job_id} -o results.zip
```

See the [User Guide](USER_GUIDE.html#api-reference) for complete API documentation.

---

## Performance

### Processing Times (Typical)

| County Size      | Origins  | Destinations | Time      |
| ---------------- | -------- | ------------ | --------- |
| Small (<50k pop) | ~5,000   | ~2,000       | 5-10 min  |
| Medium (50-200k) | ~20,000  | ~8,000       | 10-20 min |
| Large (200k-1M)  | ~100,000 | ~40,000      | 20-40 min |
| Very Large (>1M) | ~500,000 | ~200,000     | 40-90 min |

_Times vary based on hardware and data availability_

---

## Technology Stack

### Backend

- **FastAPI**: Modern Python web framework
- **GeoPandas**: Geospatial data processing
- **OSMnx**: OpenStreetMap network analysis
- **Pandas**: Data manipulation

### Frontend

- **HTML5/CSS3/JavaScript**: Modern web standards
- **Leaflet**: Interactive mapping library
- **Leaflet.heat**: Heatmap visualization

### Deployment

- **Docker**: Containerization
- **Nginx**: Reverse proxy
- **Uvicorn**: ASGI server

---

## Contributing

We welcome contributions! Here's how to get involved:

### Reporting Issues

Found a bug or have a feature request?

- [Open an issue](https://github.com/yourusername/move_od/issues)
- Provide detailed description and steps to reproduce

### Pull Requests

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Development Setup

```bash
git clone https://github.com/yourusername/move_od.git
cd move_od
pip install -r requirements.txt
pip install -r backend/requirements.txt

# Run backend
cd backend
python app.py

# Serve frontend (in another terminal)
cd frontend
python -m http.server 8080
```

---

## License

This project is licensed under the MIT License. See [LICENSE](../LICENSE) for details.

---

## Acknowledgments

### Data Sources

- **US Census Bureau**: LODES employment data and TIGER/Line shapefiles
- **Microsoft**: Open building footprint data
- **OpenStreetMap**: Geographic data (© OpenStreetMap contributors)
- **SafeGraph**: Points of interest data
- **INRIX**: Traffic speed data

### Open Source Libraries

This project builds on excellent open-source libraries including GeoPandas, OSMnx, Leaflet, FastAPI, and many others.

---

## Citation

If you use MOVE-OD in your research, please cite:

```bibtex
@software{moveod2025,
  title={MOVE-OD: Origin-Destination Data Generation Platform},
  author={Your Name},
  year={2025},
  url={https://github.com/yourusername/move_od},
  version={2.0}
}
```

---

## Support

- **📧 Email**: support@example.com
- **💬 Discussions**: [GitHub Discussions](https://github.com/yourusername/move_od/discussions)
- **🐛 Issues**: [GitHub Issues](https://github.com/yourusername/move_od/issues)
- **📚 Documentation**: [User Guide](USER_GUIDE.html)

---

## Latest Updates

### Version 2.0 (October 2025)

- 🎉 New web-based interface
- 🗺️ Interactive map visualization
- 🔥 Heatmap support
- 📊 Real-time progress tracking
- 🚀 FastAPI backend
- 📦 Docker deployment
- 🎨 Modern UI/UX

### Version 1.0 (2024)

- Initial Streamlit-based release
- Core OD generation pipeline
- LODES and SafeGraph integration
- INRIX calibration

---

<div class="cta-section">
  <h2>Ready to Get Started?</h2>
  <p>Generate high-quality origin-destination data in minutes</p>
  <a href="USER_GUIDE.html" class="cta-button">Read the User Guide</a>
  <a href="https://github.com/yourusername/move_od" class="cta-button secondary">View on GitHub</a>
</div>

---

_Last Updated: October 23, 2025_
