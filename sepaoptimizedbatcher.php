<?php

require_once 'sepaoptimizedbatcher.civix.php';

use CRM_Sepaoptimizedbatcher_ExtensionUtil as E;

/**
 * Implements hook_civicrm_config().
 *
 * @link https://docs.civicrm.org/dev/en/latest/hooks/hook_civicrm_config/
 */
function sepaoptimizedbatcher_civicrm_config(&$config): void {
  _sepaoptimizedbatcher_civix_civicrm_config($config);
}

/**
 * Implements hook_civicrm_install().
 *
 * @link https://docs.civicrm.org/dev/en/latest/hooks/hook_civicrm_install
 */
function sepaoptimizedbatcher_civicrm_install(): void {
  _sepaoptimizedbatcher_civix_civicrm_install();
}

/**
 * Implements hook_civicrm_enable().
 *
 * @link https://docs.civicrm.org/dev/en/latest/hooks/hook_civicrm_enable
 */
function sepaoptimizedbatcher_civicrm_enable(): void {
  _sepaoptimizedbatcher_civix_civicrm_enable();
}

/**
 * Implements hook_civicrm_pre().
 *
 * @link https://docs.civicrm.org/dev/en/latest/hooks/hook_civicrm_pre
 */
function sepaoptimizedbatcher_civicrm_pre($action, $entity, $id, &$params): void {
  if ($entity == 'Contribution') {
    $params['skipRecentView'] = true;
  }
}

function sepaoptimizedbatcher_civicrm_pageRun($page) {
  if ($page instanceof CRM_Sepa_Page_DashBoard) {
    $page->assign("batch_recur", CRM_Utils_System::url('civicrm/sepa/optimizedbatcher', 'update=RCUR'));
    _sepaoptimizedbatcher_check_running_jobs();
  }
}

function _sepaoptimizedbatcher_check_running_jobs() {
  $userJobs = \Civi\Api4\UserJob::get(FALSE)
  ->addSelect('id', 'row_count')
  ->addWhere('name', 'LIKE', 'sdd_update_%')
  ->addWhere('status_id:name', 'IN', ['scheduled', 'incomplete', 'in_progress'])
  ->execute();
  $count = $userJobs->countMatched();
  if ($count) {
    $job = $userJobs->first();
    $monitorUrl = CRM_Utils_System::url('civicrm/sepa/optimizedbatcher/monitor', ['user_job_id' => $job['id']]);
    CRM_Core_Session::setStatus(E::ts('There are jobs running to update recurring mandates.') . '<br><a href="' . $monitorUrl . '">' . E::ts('Monitor background job').'</a>', E::ts('Update in progress'), 'alert');
  }
}
