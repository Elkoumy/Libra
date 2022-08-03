# Libra
Libra is a framework that leverages recently developed Privacy-Enhancing Technologies (PETs) to help organizations achieve a better privacy-utility trade-off while publishing anonymized process models. Libra uses the idea of privacy amplification to reduce the injected noise while providing differential privacy guarantees. The proposed method could be adapted with any event log anonymization technique to enhance the utility.

### Prerequisite
The main dependencies are: [pm4py](https://pm4py.fit.fraunhofer.de/), [diffprivlib](https://github.com/IBM/differential-privacy-library),  and [JDK8]([https://pypi.org/project/statistics/](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html))

The code was tested with ```python 3.9.7```.

### Cite the Project

```
@inproceedings{DBLP:conf/icpm/ElkoumyD22,
  author    = {Gamal Elkoumy and
               Marlon Dumas},
  
  title     = {Libra: High-Utility Anonymization of Event Logs for Process Mining via Subsampling},
  booktitle = {4th International Conference on Process Mining, {ICPM} 2022, Bolzano,
               Italy, October 23 - October. 28, 2021},
  pages     = {80--87},
  publisher = {{IEEE}},
  year      = {2022},
  
}
  
```
