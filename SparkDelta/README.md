In order to be able to run the code in this repository, you need to install some software and packages. Follow the instructions with care. Don't skip any steps. If you have any problems, use the Teams channel to ask for help.

### 1. Install Python
1. Install the latest version of python 3.11 (3.12 gives compatibility issues) from https://www.python.org/downloads/ (WINDOWS: https://www.python.org/downloads/release/python-3119/)
2. WINDOWS: make sure to check the box "Add Python 3.11 to PATH" during installation
3. After installation, open a terminal and run the command: python --version (should be 3.11.x)

### 2. Making sure to use the correct Java version
1. If not already installed on your system, install a Java version 8 up to 17 (NEWER JAVA VERSIONS ARE NOT COMPATTIBLE!) from https://www.oracle.com/java/technologies/javase/ next to your existing Java sdk's

### 3. Loading this project in PyCharm
1. Open PyCharm
2. Select "Get from VCS"
3. Use https://gitlab.com/kdg-ti/bigdata/sparkdelta.git as the URL and provide your credentials for GitLab
4. Do not yet create a virtual environment if asked in a popup.
5. In Settings >Project Settings > Python Interpreter, select Add interpreter... > Add local interpreter > Environment New > Base: 3.11 > Location: different from your project and on a path without spaces > OK
6. Make sure this new created environment is selected as the interpreter for this project

### 4. Installing Apache Spark and Hadoop
1. Download bigdatatools.zip from Canvas
2. Unpack the downloaded file to a folder with your bigdata tools (ex. c:/tools). Unpacked directory later referred as SPARK DIRECTORY
_ATTENTION: In windows: unpack it to a (sub)folder of your main drive. Do not install it in a user folder. This causes  problems_
3. Customize the environment directories in ConnectionConfig.py file. See instructions in file.

### 5. Installing the required packages
_If PyCharm asks to install required packages when opening the project, do so. If not, follow these steps:_
1. Go to terminal in PyCharm
2. Run the command: pip install -r requirements.txt (it includes all the necessary packages for this course)
3. Make sure all the packages are installed correctly (if you have errors, try to solve them and run the command again)
4. If the IDE asks to install jupyter, do so.

### 6. Install Big Data Tools plugin
_PyCharm has a set of plugins that helps you running Spark Applications._
2. Search for Big Data Tools and install the plugin

### 7. Performing the first testruns
1. Open the CHECK_1_install.ipynb file in PyCharm
2. Run all cells one by one and check if everything is working correctly
3. If you have errors, try to solve them and run the cells again.
4. If you are stuck and cannot solve the errors, contact the teacher.
5. If everything is working correctly, you are ready to start the course!

