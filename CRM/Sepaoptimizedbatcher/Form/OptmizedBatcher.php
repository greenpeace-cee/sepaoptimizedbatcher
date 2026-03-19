<?php

use Civi\Sepa\Lock\SepaBatchLockManager;
use CRM_Sepaoptimizedbatcher_ExtensionUtil as E;

class CRM_Sepaoptimizedbatcher_Form_OptmizedBatcher extends CRM_Core_Form {

  public function buildQuickForm()
  {
    _sepaoptimizedbatcher_check_running_jobs();
    $this->addCheckBox('what', E::ts('What to do?'), [
      'repair' => E::ts('Repair'),
      'collection_date' => E::ts('Calculate next collection date of mandates'),
      'update' => E::ts('Create contributions and sepa groups'),
    ], NULL, NULL, NULL, NULL, '<br />', TRUE);

    $this->addButtons(array(
      array(
        'type' => 'submit',
        'name' => E::ts('Update Recurring'),
        'isDefault' => TRUE,
      ),
    ));
  }

  public function setDefaultValues()
  {
    return ['what' => ['collection_date' => 1, 'update' => 1]];
  }

  public function postProcess()
  {
    $what = $this->getSubmittedValue('what');
    if (!SepaBatchLockManager::getInstance()->acquire(0)) {
      CRM_Core_Session::setStatus(E::ts('Cannot run update, another update is in progress!'), '', 'error');
      CRM_Utils_System::redirect(CRM_Utils_System::url('civicrm/sepa/dashboard', 'status=active'));
    }
    // use the runner rather that the API (this doesn't return)
    CRM_Sepaoptimizedbatcher_Logic_Queue::launchUpdateRunner($what);

    CRM_Utils_System::redirect(CRM_Utils_System::url('civicrm/sepa/dashboard', 'status=active'));
  }

}
