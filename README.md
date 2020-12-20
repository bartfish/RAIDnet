# RAIDnet
DotNet framework allowing for implementation of RAID 0+1 into any .NET database-based application


# What is it exactly?
Each application usually has its own database and operates based on its contents. When the user is working on the app, searching, loading, sending information from/to database, the application sends it back to the user. However, when part of the database is infected by some virus, deleted or removed, the content of the application does not load or some error occurs. The solution of the problem should prevent such thing from happening. In other words, user should not notice any backend trouble, even it exists simultaneously while the app is used. That said, the backend mechanism should recover not working part of the database and it should do it „quietly”, so the user do not feel any discomfort while using the application.


# Short description of the solution
The framework provides data distribution, synchronization and switching based on RAID 01 with the support of data recovery, allowing all functionalities to work during the runtime of the application to which the framework is implemented. Moreover, the disturbance of the end user of the application is avoided, while any issues happen on the persistence side of the application, due to the framework’s design and architecture of implementation.
