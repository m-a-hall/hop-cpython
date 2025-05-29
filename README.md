Hop CPython Plugin
=======================

The Hop CPython Project is a plugin for the Apache Hop platform which provides the ability to execute a python script (via the cpython environment) within the context of a pipeline.

Building
--------
The Hop CPython Plugin is built with Maven.

    $ git clone https://github.com/m-a-hall/hop-cpython.git
    $ cd hop-cpython
    $ mvn clean install

This will produce a plugin archive in target/hop-cpython-${version}.zip (e.g., hop-cpython-2.13.0-GA.zip).

Installation
------------
1. **Install the Plugin:**
   Extract the generated hop-cpython-2.13.0-GA.zip file into your Apache Hop installation's `/hop/plugins/transforms` folder.

2. **Set up Python Environment:**
   Create a Python virtual environment and install the required packages:
   ```bash
   # Create virtual environment
   python -m venv hop-cpython-env
   
   # Activate virtual environment
   # On Windows:
   hop-cpython-env\Scripts\activate
   # On macOS/Linux:
   source hop-cpython-env/bin/activate
   
   # Install required packages
   pip install -r requirements.txt
   ```
   
   Alternatively, you can install the packages individually:
   ```bash
   pip install pandas numpy scipy matplotlib scikit-learn pyarrow>=15.0.0
   ```

Architecture
-----------
The plugin uses a client-server architecture where the Hop pipeline (Java) communicates with a Python process via socket connections. The diagram below illustrates the communication flow:

![Hop CPython Communication Flow](cpython-diagram.png)

Apache Arrow Support
-------------------
Version 2.13.0 adds support for Apache Arrow for high-performance data transfer between Hop and Python. Arrow provides:
- Significantly faster data transfer for large datasets
- Zero-copy data exchange
- Better handling of data types and null values
- Automatic fallback to CSV when Arrow is not available

To enable Arrow support, install pyarrow in your Python environment:
```
pip install pyarrow>=15.0.0
```

Arrow support is enabled by default when available and can be configured in the transform settings.

System Requirements
------------------
- Python 3.6 or higher (Python 2 is no longer supported)
- Java 17 (not higher)
- Apache Hop 2.7.0 or later

**Required Python packages:**
- pandas (for data manipulation)
- numpy (for numerical computing)
- scipy (for scientific computing)
- matplotlib (for data visualization)
- scikit-learn (for machine learning)
- pyarrow>=15.0.0 (for high-performance data transfer)

All required packages and their dependencies are listed in `requirements.txt`. The Anaconda distribution of Python is a convenient option (especially for Windows users) as it includes most of these packages pre-installed.

License
-------
Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.