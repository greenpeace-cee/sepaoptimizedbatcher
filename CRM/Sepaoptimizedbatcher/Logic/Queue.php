<?php

use Civi\Api4\UserJob;
use CRM_Sepaoptimizedbatcher_ExtensionUtil as E;
use Civi\Sepa\Lock\SepaBatchLockManager;

/**
 * @author Jaap Jansma <jaap.jansma@civicoop.org>
 * @license AGPL-3.0
 */

class CRM_Sepaoptimizedbatcher_Logic_Queue {

  private const BATCH_SIZE = 250;

  public string $title;
  public $runAs;
  private string $cmd;
  private string $mode;
  private $creditorId;
  private ?int $offset;
  private ?int $limit;

  private string $asyncLockId;

  public static function createUserJob(): int {
    $id = UserJob::create(FALSE)
      ->setValues([
        'created_id' => CRM_Core_Session::getLoggedInContactID(),
        'job_type' => 'sepaoptimizedbatcher',
        'status_id:name' => 'draft',
        // This suggests the data could be cleaned up after this.
        'expires_date' => '+ 1 week',
        'metadata' => [],
      ])
      ->execute()
      ->first()['id'];
    return $id;
  }


  /**
   * Create a task for the queue
   *
   * @param $cmd string
   * @param $params array
   * @return CRM_Queue_Task
   */
  private static function createTask($cmd, $params = []) {
    $task = new CRM_Queue_Task(
      ['CRM_Sepaoptimizedbatcher_Logic_Queue', 'run'],
      [$cmd, $params],
      self::taskTitle($cmd, $params)
    );

    $task->runAs = [
      'contactId' => CRM_Core_Session::getLoggedInContactID(),
      'domainId'  => 1,
    ];

    return $task;
  }

  public static function launchUpdateRunner(array $what) {
    $userJobId = static::createUserJob();
    $asyncLockId = uniqid('', TRUE);
    if (!SepaBatchLockManager::getInstance()->acquire(0, $asyncLockId)) {
      SepaBatchLockManager::getInstance()->release($asyncLockId);
      CRM_Core_Session::setStatus(E::ts('Cannot run update, another update is in progress!'), E::ts('Error'), 'error');
      $redirectUrl = CRM_Utils_System::url('civicrm/sepa/dashboard', 'status=active');
      CRM_Utils_System::redirect($redirectUrl);
      return; // shouldn't be necessary
    }
    
    // create a queue
    $queue = Civi::queue('sdd_update_sequential', [
      'error'      => 'abort',
      'lease_time' => 60 * 60 * 24, // 24 hours
      'reset'      => TRUE,
      'runner'     => 'task',
      'type'       => 'Sql',
      'user_job_id' => $userJobId
    ]);

    // first thing: close outdated groups
    $queue->createItem(self::createTask('CLOSE', ['mode' => 'RCUR']), ['weight' => 0]);

    // then iterate through all creditors
    $creditors = civicrm_api3('SepaCreditor', 'get', array('option.limit' => 0));
    foreach ($creditors['values'] as $creditor) {
      $sdd_modes = array('FRST', 'RCUR');
      foreach ($sdd_modes as $sdd_mode) {
        $count = self::getMandateCount($creditor['id'], $sdd_mode);
        if (!empty($what['repair']) || !empty($what['collection_date'])) {
          for ($offset=0; $offset < $count; $offset+=self::BATCH_SIZE) {
            if (!empty($what['repair'])) {
              $queue->createItem(self::createTask('REPAIR', ['mode' => $sdd_mode, 'creditor_id' => $creditor['id'], 'offset' => $offset, 'limit' => self::BATCH_SIZE, 'count' => $count]), ['weight' => 1]);
            }
            if (!empty($what['collection_date'])) {
              $queue->createItem(self::createTask('COLLECTION_DATE', ['mode' => $sdd_mode, 'creditor_id' => $creditor['id'], 'offset' => $offset, 'limit' => self::BATCH_SIZE, 'count' => $count]), ['weight' => 2]);
            }
          }  
        }
      }
    }

    if (!empty($what['update'])) {
      $queue->createItem(self::createTask('PREPARE_UPDATE', []), ['weight' => 3]);
    }

    UserJob::update(FALSE)
      ->setValues([
        'name' => 'sdd_update_sequential_' . $userJobId,
        'queue_id.name' => 'sdd_update_sequential',
        'status_id:name' => 'scheduled',
      ])
      ->addWhere('id', '=', $userJobId)
      ->execute();


    // create a runner and launch it
    $runner = new CRM_Queue_Runner(array(
      'title'     => E::ts("Updating %1 SEPA Groups", array(1 => 'RCUR')),
      'queue'     => $queue,
      'errorMode' => CRM_Queue_Runner::ERROR_ABORT,
      'onEndUrl'  => CRM_Utils_System::url('civicrm/sepa/dashboard', 'status=active', FALSE, NULL, FALSE),
    ));

    $runner->runAllInteractive(); // does not return
  }

    /**
   * Render a title for a task
   *
   * @param $cmd string
   * @param $params array
   * @return string
   */
  private static function taskTitle($cmd, $params) {
    $creditor_id = $params['creditor_id'] ?? NULL;
    $mode = $params['mode'] ?? '[unknown]';
    $offset = $params['offset'] ?? 0;
    $limit = $params['limit'] ?? 0;
    $count = $params['count'] ?? 0;

    switch ($cmd) {
      case 'CLEANUP':
        return E::ts("Cleaning up $mode groups");

      case 'CLOSE':
        return E::ts('Cleaning up ended mandates');

      case 'PREPARE':
        return E::ts('Preparing to clean up ended mandates');

      case 'REPAIR':  
        return E::ts("Repair %1 mandates (%2-%3) / %4", [1 => $mode, 2 => $offset, 3 => $offset+$limit, 4=> $count]);
        break;

      case 'COLLECTION_DATE':
        return E::ts("Determine colledtion dates of %1 mandates (%2-%3) / %4", [1 => $mode, 2 => $offset, 3 => $offset+$limit, 4=> $count]);
        break;  
      
      case 'PREPARE_UPDATE':
        return E::ts("Prepare processing %1 mandates",[1 => $mode]);
        break;  

      case 'UPDATE':
        return E::ts("Process $mode mandates (%1-%2)", [1 => $offset,2 => $offset + $limit,]);
        break;

      case 'WATCH_UPDATE':
        return E::ts("Watch $mode for updating mandates", [1 => $offset,2 => $offset + $limit,]);
        break;  

      default:
        return E::ts('Unknown');
        break;
    }
  }

  public static function run($context, $cmd, $params): bool {
    $creditorId = $params['creditor_id'] ?? NULL;
    $mode = $params['mode'] ?? '[unknown]';
    $offset = $params['offset'] ?? 0;
    $limit = $params['limit'] ?? 0;

    \CRM_Core_Config::setPermitCacheFlushMode(FALSE);

    switch ($cmd) {
      case 'CLOSE':
        CRM_Sepa_Logic_Batching::closeEnded();
        break;

      case 'REPAIR':
        CRM_Sepaoptimizedbatcher_Logic_Batching::repairRCUR($creditorId, $mode, 'now', $offset, $limit);
        break;

      case 'COLLECTION_DATE':
        CRM_Sepaoptimizedbatcher_Logic_Batching::updateNextScheduledDateRCUR($creditorId, $mode, 'now', $offset, $limit);
        break;
      
      case 'PREPARE_UPDATE':
        $batchSize = self::BATCH_SIZE;
        $queue = $context->queue;
        $bgqueue_enabled = (bool) Civi::settings()->get('enableBackgroundQueue');
        $weight = 4;
        if ($bgqueue_enabled) {
          // Create a parralel queue for parralel processing.
          // This queue is executed by coworker.
          // We have a separate job to check whether the queue is done.
          $parralelQueueName = 'sdd_update_parallel';
          $queue->createItem(self::createTask('WATCH_UPDATE', ['queue_name_to_check' => $parralelQueueName]), ['weight' => 4]);
          $queue = Civi::queue($parralelQueueName, [
            'error'  => 'abort',
            'reset'  => TRUE,
            'runner' => 'task',
            'type'   => 'SqlParallel',
          ]);
          $weight = 0;
          $batchSize = 1;
        }

        $now = 'now';
        $creditors = civicrm_api3('SepaCreditor', 'get', array('option.limit' => 0));
        foreach ($creditors['values'] as $creditor) {
          $horizon = (int) CRM_Sepa_Logic_Settings::getSetting("batching.RCUR.horizon", $creditor['id']);
          $latest_date = date('Y-m-d', strtotime("$now +$horizon days"));
          $sdd_modes = array('FRST', 'RCUR');
          foreach ($sdd_modes as $sdd_mode) {
            $relevantMandateCount = \Civi\Api4\SepaMandate::get(TRUE)
              ->selectRowCount()
              ->addJoin('ContributionRecur AS contribution_recur','INNER', ['entity_table', '=', '"civicrm_contribution_recur"'],['entity_id', '=', 'contribution_recur.id'])
              ->addJoin('Contribution AS first_contribution','LEFT',['first_contribution_id', '=', 'first_contribution.id'])
              ->addWhere('type', '=', 'RCUR')
              ->addWhere('status', '=', $sdd_mode)
              ->addWhere('creditor_id', '=', $creditor['id'])
              ->addWhere('contribution_recur.next_sched_contribution_date', '<=', $latest_date)
              ->execute()
              ->countMatched();
            for ($offset=0; $offset < $relevantMandateCount; $offset+=$batchSize) {
              // add an item for each batch
              $queue->createItem(self::createTask('UPDATE', ['mode'=>$sdd_mode, 'creditor_id' => $creditor['id'], 'offset' => $offset, 'limit' => $batchSize, 'count' => $relevantMandateCount]), ['weight' => $weight]);
            }
          }
        }
        $queue->createItem(self::createTask('CLEANUP', ['mode' => 'FRST']), ['weight' => 999]);
        $queue->createItem(self::createTask('CLEANUP', ['mode' => 'RCUR']), ['weight' => 999]);
        break;
      
      case 'WATCH_UPDATE':
        $parralelQueueName = $params['queue_name_to_check'];
        $queueStatus = CRM_Core_DAO::singleValueQuery("SELECT `status` FROM civicrm_queue WHERE `name` = %1", [1=>[$parralelQueueName, 'String']]);
        if ($queueStatus == 'aborted') {
          throw new \CRM_Core_Exception(E::ts('Error in updating mandates in parralel'));
        }   
        $taskCount = (int) CRM_Core_DAO::singleValueQuery("SELECT count(*) FROM civicrm_queue_item WHERE queue_name = %1", [1=>[$parralelQueueName, 'String']]);
        if ($taskCount > 0) {
          $context->queue->createItem(self::createTask('WATCH_UPDATE', ['queue_name_to_check' => $parralelQueueName]), ['weight' => 4]);
          sleep(30); // Wait for 30 seconds before checking again.
        }
        break;  

      case 'UPDATE':
        CRM_Sepaoptimizedbatcher_Logic_Batching::updateRCUR($creditorId, $mode, 'now', $offset, $limit);
        break;

      case 'CLEANUP':
        CRM_Sepa_Logic_Group::cleanup($mode);
        break;

      default:
        return FALSE;
    }

    return TRUE;
  }

    /**
   * determine the count of mandates to be investigated
   */
  protected static function getMandateCount($creditor_id, $sdd_mode) {
    if ($sdd_mode == 'OOFF') {
      $horizon = (int) CRM_Sepa_Logic_Settings::getSetting('batching.OOFF.horizon', $creditor_id);
      $date_limit = date('Y-m-d', strtotime("+$horizon days"));
      return CRM_Core_DAO::singleValueQuery("
        SELECT COUNT(mandate.id)
        FROM civicrm_sdd_mandate AS mandate
        INNER JOIN civicrm_contribution AS contribution  ON mandate.entity_id = contribution.id
        WHERE contribution.receive_date <= DATE('$date_limit')
          AND mandate.type = 'OOFF'
          AND mandate.status = 'OOFF'
          AND mandate.creditor_id = $creditor_id;");
    } else {
      return CRM_Core_DAO::singleValueQuery("
        SELECT
          COUNT(mandate.id)
        FROM civicrm_sdd_mandate AS mandate
        WHERE mandate.type = 'RCUR'
          AND mandate.status = '$sdd_mode'
          AND mandate.creditor_id = $creditor_id;");
    }
  }

}