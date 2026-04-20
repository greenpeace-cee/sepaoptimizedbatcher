<?php

use Civi\Api4\UserJob;
use CRM_Sepaoptimizedbatcher_ExtensionUtil as E;

/**
 * @author Jaap Jansma <jaap.jansma@civicoop.org>
 * @license AGPL-3.0
 */

class CRM_Sepaoptimizedbatcher_Page_Monitor extends CRM_Core_Page {

  public function run() {
    CRM_Utils_System::setTitle(E::ts('Monitor background job of updating recurring'));
    $userJobId = CRM_Utils_Request::retrieve('user_job_id', 'Integer');
    $job = UserJob::get(FALSE)->addWhere('id', '=', $userJobId)->execute()->first();
    $totalNumberOfTasks = $job['metadata']['totalNumberOfTasks'] ?? 0;
    $numberOfTasksExecuted = $job['metadata']['numberOfTasksExecuted'] ?? 0;
    $url = CRM_Utils_System::url('civicrm/sepa/optimizedbatcher/monitor', ['snippet' => 'json', 'user_job_id' => $userJobId]);

    $pct = 100;
    if ($totalNumberOfTasks > 0 && $job['status_id'] == 3) {
      $pct = 100 * $numberOfTasksExecuted / $totalNumberOfTasks;
    }
    $this->assign('pct', $pct);
    $this->assign('url', $url);
    $this->assign('description', E::ts('The recurring contributions will be created in a background process. If you wish you can close this window.'));
    $this->ajaxResponse['pct'] = $pct;
    if ($pct >= 100) {
      $this->ajaxResponse['redirect_url'] = CRM_Utils_System::url('civicrm/sepa/dashboard', 'status=active');
    }
    parent::run();
  }

}