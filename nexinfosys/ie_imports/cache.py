
"""
Logic to manage the cache for external datasets

Features per dataset or per dataset source (shared by different datasets)
* Refresh times for the dataset
  * Conditions expiry time of cached information
* Dataset identification
* Eager (automatic) refresh or lazy refresh (on demand)
* Maximum size
"""