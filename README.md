# SEPA Optimized Batcher

This extension improves the performance updating recurring contributions for CiviSepa. It does two things:

1. Improves the performance for updating recurring contribution in CiviSepa by using direct SQL and only creating contributions for mandates within the horizon
2. The update recurring can use a background process such as [coworker](https://lab.civicrm.org/dev/coworker). When coworker is installed and backrgound processing is enabled in CiviCRM the contributions are generated in parralel.

This is an [extension for CiviCRM](https://docs.civicrm.org/sysadmin/en/latest/customize/extensions/), licensed under [AGPL-3.0](LICENSE.txt).
