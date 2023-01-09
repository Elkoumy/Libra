# Libra
Libra is a framework that leverages recently developed Privacy-Enhancing Technologies (PETs) to help organizations achieve a better privacy-utility trade-off while publishing anonymized process models. Libra uses the idea of privacy amplification to reduce the injected noise while providing differential privacy guarantees. The proposed method could be adapted with any event log anonymization technique to enhance the utility.

### Prerequisite
The main dependencies are: [pm4py](https://pm4py.fit.fraunhofer.de/), [diffprivlib](https://github.com/IBM/differential-privacy-library), [multiprocessing](https://pypi.org/project/multiprocess/) and [statistics](https://pypi.org/project/statistics/)
You can install all the requirements with:
```
pip install -r requirements.txt
```

### Availability 
Libra is available as a python package. To anonymize an event log, place the XES file in the directory ```data```. Then you can run the command
```
python Libra.py "BPIC12_t" 2 0.05 10 20 0.001
```
The above parameters are : ```event log```, ```b```, ```gamma```, ```alpha```, ```epsilon_in_minutes```, ```delta```.
Libra assumes that the event log has only the three columns: ```case:concept:name```, ```concept:name```, and ```time:timestamp``` in your XES file.
For more information about the description of these parameters, please check out the [paper](https://ieeexplore.ieee.org/abstract/document/9980619).


### Cite the Project

```
@inproceedings{DBLP:conf/icpm/ElkoumyD22,
  author    = {Gamal Elkoumy and
               Marlon Dumas},
  editor    = {Andrea Burattin and
               Artem Polyvyanyy and
               Barbara Weber},
  title     = {Libra: High-Utility Anonymization of Event Logs for Process Mining
               via Subsampling},
  booktitle = {4th International Conference on Process Mining, {ICPM} 2022, Bolzano,
               Italy, October 23-28, 2022},
  pages     = {144--151},
  publisher = {{IEEE}},
  year      = {2022},
  url       = {https://doi.org/10.1109/ICPM57379.2022.9980619},
  doi       = {10.1109/ICPM57379.2022.9980619},
  timestamp = {Tue, 03 Jan 2023 07:35:57 +0100},
  biburl    = {https://dblp.org/rec/conf/icpm/ElkoumyD22.bib},
  bibsource = {dblp computer science bibliography, https://dblp.org}
}
  
```
