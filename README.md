<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://convida.inf.um.es">
    <img src="img/convida-logo.png" alt="Logo" width="220" height="80">
  </a>

  <h3 align="center">COnVIDa library</h3>

  <p align="center">
    Collection of different data sources in the context of COVID19
    <br />
  </p>
</p>



<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
* [Roadmap](#roadmap)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)
* [Acknowledgements](#acknowledgements)



<!-- ABOUT THE PROJECT -->
## About the project

COnVIDa (convida.inf.um.es) is a tool developed by the Cybersecurity and Data Science Laboratory at the University of Murcia (Spain) that allows easily gathering data related to the COVID19 pandemic form different data sources, in the context of Spain, and visualize them in a graph.

In particular, this project contains the python library which is being developed to collect data from the different data sources (which, in turn, is being used in the backend of COnVIDa service). The code is publicly available to be used by researchers, data analysis and software developers, ready to be used as modules in python scripts or IPython Notebooks. Moreover, is specially designed to be modular and extensible to new data sources.



<!-- GETTING STARTED -->
## Getting Started

COnVIDa library is particularly composed by two main elements:
* ```/lib```: Core implementation of the crawling functionality.  
* ```/server```: Example of the use of the library itself, when deploying it as a service.

### Prerequisites

* Python3
* pip3


### Installation

1. Clone the repo
```sh
git clone https://github.com/CyberDataLab/COnVIDa-lib.git
```
2. Change to COnVIDa-lib directory
3. Install required packages (using a virtual environment such as ```conda``` is highly recommended)
```sh
pip3 install -r requirements.txt
```



<!-- USAGE EXAMPLES -->
## Usage

### ```lib```

The library can be easily used as shown in the [test lib notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/test_lib.ipynb). Some considerations should be taken:

* The import of ConVIDa modules should be addressed accordingly. The simplest way would be to place your script or Notebook in ```lib``` folder. However, you are free to manage the imports as [desired](https://docs.python.org/3/reference/import.html).

* The class _COnVIDa_ acts as a factory which encapsulates the low-level implementation of the different data sources. In this sense, for the usage of this library is only necessary to know its public functions. For more info, see [lib documentation](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/).

* The _COnVIDa_ library always access external sources to retrieve the data. In this sense, it is worth keeping the data on disk to avoid requesting the same data several times. In fact, the ```server``` example is specifically oriented to work with the data locally once a data cache is built by using ```lib```.


### ```server```
The service can be easily used as shown in the [test server notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/test_lib.ipynb). Some considerations should be taken:


* The import of ConVIDa server should be addressed accordingly. The simplest way would be to place your script or Notebook in ```server``` folder. However, you are free to manage the imports as [desired](https://docs.python.org/3/reference/import.html).

* The class _convida_server_ also manages user queries from a high-level perspective. However, these queries are in these case locally filtered against a data cache file (which is generated for the first time with [data generation notebook](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/data_generation.ipynb) and can be updated with _daily_update_ function). For more info, see [server documentation](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/server/).


<!-- ROADMAP -->
## Roadmap

See the [open issues](https://github.com/CyberDataLab/COnVIDa-lib/issues) for a list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. _COnVIDa_ library is specially designed to be extended with little effort, see the [developer guidelines](https://github.com/CyberDataLab/COnVIDa-lib/blob/master/lib/) for more info.


Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

CyberDataLab - [@cyberdatalab](https://twitter.com/cyberdatalab) 

Contact us through convida@listas.um.es

Entire COnVIDa project: [https://github.com/CyberDataLab/COnVIDa-dev](https://github.com/CyberDataLab/COnVIDa-dev)



<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements
* [Datadista](https://github.com/datadista/datasets)
