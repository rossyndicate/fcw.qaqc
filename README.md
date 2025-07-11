# fcw.qaqc

## Overview

`fcw.qaqc` provides a comprehensive suite of functions for quality assurance and quality control (QAQC) of water quality sensor data. The package implements a multi-level flagging system that combines single-parameter checks, inter-parameter validation, and network-wide analysis to ensure data quality and reliability.

This package was specifically developed for the Radical Open Science Syndicate's network of In-Situ Inc. water quality sondes deployed throughout the Poudre River watershed in Colorado. The QAQC functions, API integrations (mWater and HydroVu), and flagging algorithms have been fine-tuned to address the unique challenges and data patterns observed in this particular monitoring network. However, the modular design and flexible configuration system make this package adaptable for other water quality monitoring networks. Users can customize threshold files, modify API connections, adjust flagging parameters, and extend the QAQC functions to accommodate different sensor types, environmental conditions, and data management systems.

## Key Features

-   **Multi-source data integration**: Seamlessly combines data from mWater and HydroVu APIs
-   **Automated QAQC workflow**: Comprehensive flagging system with minimal manual intervention
-   **Multi-level validation**:
    -   Single-parameter flags (drift, noise, repeating values, spec violations)
    -   Inter-parameter flags (frozen conditions, burial, unsubmerged sensors)
    -   Network-level flags (cross-site comparisons, suspect data identification)
-   **Parallel processing**: Efficient handling of large datasets across multiple sites
-   **Flexible configuration**: Customizable thresholds, time intervals, and processing parameters

## Installation

Install the development version from GitHub:

``` r
# Install devtools if you haven't already
install.packages("devtools")

# Install fcw.qaqc
devtools::install_github("yourusername/fcw.qaqc")
```
