""""
Functions to perform the needed API calls to MAGIC's official web site, which is built using Drupal

* Create a case study (first a "case study" entity has to be defined)
* Synchronize case study. For this, after creation, store Drupal's ID. Then, assuming Drupal has APIs for Update instead Create, call the corresponding resources

The elements of the "case study" can be fields from the Metadata, static graphs, tables, ...

Documentation of Drupal's "Entity API" is at:

https://api.drupal.org/api/drupal/core%21lib%21Drupal%21Core%21Entity%21entity.api.php/group/entity_api/8.5.x

"""