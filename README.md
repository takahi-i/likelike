likelike
========

An implementation of locality sensitive hashing

Overview
============

Likelike is an implementation of LSH (locality sensitive hashing) on Hadoop. This program can be used for the nearest neighbor extraction or item recommendation in E-commerce sites. Currently Likelike supports only Min-Wise independent permutations. Min-Wise independent permutations is applied to the recommendation of Google News (Das et al. 2007).

Usage
========
Begin with the Likelike quick start page ([QuickStart](https://github.com/takahi-i/likelike/wiki/Qick-Start)) which provides the information on the installation and tutorial with small input files. For detailed usage, please visit the [Usage](https://github.com/takahi-i/likelike/wiki/Usage) page.

News
======
- 2015-02-03 support Hadoop v2.4
- 2013-01-01 port the code into Github
- 2011-09-26 gave a presentation in the Hadoop conference Japan Fall (slides)
- 2011-08-22 likelike 0.3.0 released
switched the build system from Ant into Maven.
- 2010-05-06 likelike 0.2.0 released

Requirements
===============
- Java 1.７.0 or greater
- Hadoop 2.4 or greater

Applications
==============
Likelike can be applied to following tasks.

- item recommendation in E-commerce sites
- similar documents (or images) extraction
- news recommendation from click through log data

To do
========
- provide detailed usage page
- provide server function
- support feature selection
- performance tuning

Author
==========
Takahiko Ito <takahiko03 --at-- gmail.com>

References
===========
- Edith Cohen. Size-Estimation Framework with Applications to Transitive Closure and Reachability. Jour. of Computerand System Sciences. 1997.
- Abhinandan S. Das, Mayur Datar, Ashutosh Garg and Shyam Rajaram, Google news personalization: scalable online collaborative filtering, In: Proc. of WWW conference, 2007.
- Indyk Piotr and Motwani Rajeev. Approximate Nearest Neighbors: Towards Removing the Curse of Dimensionality. In: Proc. of Symposium on Theory of Computing. 1998.
