Hop CPython Plugin
=======================

The Hop CPython Project is a plugin for the Apache Hop platform which provides the ability to execute a python script (via the cpython environment) within the context of a pipeline.

Building
--------
The Hop CPython Plugin is built with Maven.

    $ git clone https://github.com/m-a-hall/hop-cpython.git
    $ cd hop-cpython
    $ mvn clean install

This will produce a plugin archive in target/hop-cpython-${version}.zip. This archive can then be extracted into your Hop plugin/transforms directory.

Further Reading
---------------
You will need to have python installed on your machine. Either >= 2.7 in the 2.x version of python or 3.x. In addition, the following python packages are required:

pandas
numpy
scipy
matplotlib

The Anaconda distribution of python is a simple way to get started (especially for Windows users) as it comes with hundreds of packages pre-installed.

License
-------
Licensed under the Apache License, Version 2.0. See LICENSE.txt for more information.
