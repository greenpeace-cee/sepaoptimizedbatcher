<?php

use Civi\Api4\ContributionRecur;
use Civi\Sepa\Lock\SepaBatchLockManager;

/**
 * @author Jaap Jansma <jaap.jansma@civicoop.org>
 * @license AGPL-3.0
 */

class CRM_Sepaoptimizedbatcher_Logic_Batching {

  public static function repairRCUR($creditor_id, $mode, $now = 'now', $offset=NULL, $limit=NULL) {
    // check lock
    /*$lock = SepaBatchLockManager::getInstance()->getLock();
    if (!$lock->acquire()) {
      return "Batching in progress. Please try again later.";
    }*/

    if ($offset !== NULL && $limit!==NULL) {
      $batch_clause = "LIMIT {$limit} OFFSET {$offset}";
    }
    else {
      $batch_clause = "";
    }

    // RCUR-STEP 0: check/repair mandates
    CRM_Sepa_Logic_MandateRepairs::runWithMandateSelector(
      "mandate.type = 'RCUR' AND mandate.status = '{$mode}' AND mandate.creditor_id = {$creditor_id} {$batch_clause}",
      true
    );
  }

  public static function updateNextScheduledDateRCUR($creditor_id, $mode, $now = 'now', $offset=NULL, $limit=NULL) {
    $grace_period = (int) CRM_Sepa_Logic_Settings::getSetting("batching.RCUR.grace", $creditor_id);
    $rcur_notice = (int) CRM_Sepa_Logic_Settings::getSetting("batching.$mode.notice", $creditor_id);
    // (virtually) move ahead notice_days, but also go back grace days
    $now = strtotime("$now +$rcur_notice days -$grace_period days");
    $now = strtotime(date('Y-m-d', $now));
    // RCUR-STEP 1: find all active/pending RCUR mandates within the horizon that are NOT in a closed batch and that
    // have a corresponding contribution of a financial type the user has access to (implicit condition added by
    // Financial ACLs extension if enabled)
    $relevant_mandates = \Civi\Api4\SepaMandate::get(TRUE)
      ->addSelect(
        'id',
        'contact_id',
        'entity_id',
        'source',
        'creditor_id',
        'first_contribution.receive_date',
        'contribution_recur.cycle_day',
        'contribution_recur.frequency_interval',
        'contribution_recur.frequency_unit',
        'contribution_recur.start_date',
        'contribution_recur.cancel_date',
        'contribution_recur.end_date',
        'contribution_recur.amount',
        'contribution_recur.is_test',
        'contribution_recur.contact_id',
        'contribution_recur.financial_type_id',
        'contribution_recur.contribution_status_id',
        'contribution_recur.currency',
        'contribution_recur.campaign_id',
        'contribution_recur.payment_instrument_id',
        'contribution_recur.next_sched_contribution_date'
      )
      ->addJoin(
        'ContributionRecur AS contribution_recur',
        'INNER',
        ['entity_table', '=', '"civicrm_contribution_recur"'],
        ['entity_id', '=', 'contribution_recur.id']
      )
      ->addWhere('type', '=', 'RCUR')
      ->addWhere('status', '=', $mode)
      ->addWhere('creditor_id', '=', $creditor_id)
      ->setLimit($limit)
      ->setOffset($offset)
      ->addOrderBy('contribution_recur.next_sched_contribution_date', 'ASC')
      ->execute()
      ->getArrayCopy();

    foreach ($relevant_mandates as $mandate) {
      $mandate += [
        'mandate_id' => $mandate['id'],
        'mandate_contact_id' => $mandate['contact_id'],
        'mandate_entity_id' => $mandate['entity_id'],
        'mandate_first_executed' => $mandate['first_contribution.receive_date'],
        'mandate_source' => $mandate['source'],
        'mandate_creditor_id' => $mandate['creditor_id'],
        'cycle_day' => $mandate['contribution_recur.cycle_day'],
        'frequency_interval' => $mandate['contribution_recur.frequency_interval'],
        'frequency_unit' => $mandate['contribution_recur.frequency_unit'],
        'start_date' => $mandate['contribution_recur.start_date'],
        'end_date' => $mandate['contribution_recur.end_date'],
        'cancel_date' => $mandate['contribution_recur.cancel_date'],
        'rc_contact_id' => $mandate['contribution_recur.contact_id'],
        'rc_amount' => $mandate['contribution_recur.amount'],
        'rc_currency' => $mandate['contribution_recur.currency'],
        'rc_financial_type_id' => $mandate['contribution_recur.financial_type_id'],
        'rc_contribution_status_id' => $mandate['contribution_recur.contribution_status_id'],
        'rc_campaign_id' => $mandate['contribution_recur.campaign_id'] ?? NULL,
        'rc_payment_instrument_id' => $mandate['contribution_recur.payment_instrument_id'],
        'rc_is_test' => $mandate['contribution_recur.is_test'],
      ];
      // RCUR-STEP 2: calculate next execution date
      $next_date = CRM_Sepa_Logic_Batching::getNextExecutionDate($mandate, $now, ($mode=='FRST'));
      if (NULL !== $next_date) {
        $deferred_collection_date = $next_date;
        CRM_Sepa_Logic_Batching::deferCollectionDate($deferred_collection_date, $creditor_id);
        if ($deferred_collection_date != $next_date) {
          $next_date = $deferred_collection_date;
        }
      }
      if ($next_date !== $mandate['contribution_recur.next_sched_contribution_date']) {
        ContributionRecur::update(TRUE)
          ->addValue('next_sched_contribution_date', $next_date)
          ->addWhere('id', '=', $mandate['contribution_recur.id'])
          ->execute();
      }
    }
  }

  public static function updateRCUR($creditor_id, $mode, $now = 'now', $offset=NULL, $limit=NULL) {
    // check lock
    /*$lock = SepaBatchLockManager::getInstance()->getLock();
    if (!$lock->acquire()) {
      return "Batching in progress. Please try again later.";
    }*/

    $horizon = (int) CRM_Sepa_Logic_Settings::getSetting("batching.RCUR.horizon", $creditor_id);
    $latest_date = date('Y-m-d', strtotime("$now +$horizon days"));

    $rcur_notice = (int) CRM_Sepa_Logic_Settings::getSetting("batching.$mode.notice", $creditor_id);
    $group_status_id_open = (int) CRM_Core_PseudoConstant::getKey('CRM_Batch_BAO_Batch', 'status_id', 'Open');

    // get payment instruments
    $payment_instruments = CRM_Sepa_Logic_PaymentInstruments::getPaymentInstrumentsForCreditor($creditor_id, $mode);
    $payment_instrument_id_list = implode(',', array_keys($payment_instruments));
    if (empty($payment_instrument_id_list)) {
      return; // disabled
    }

    $sql = "SELECT `civicrm_sdd_mandate`.`id`,
        `civicrm_sdd_mandate`.`contact_id`,
        `civicrm_sdd_mandate`.`entity_id`,
        `civicrm_sdd_mandate`.`source`,
        `civicrm_sdd_mandate`.`creditor_id`,
        `first_contribution`.`receive_date`,
        `contribution_recur`.`cycle_day`,
        `contribution_recur`.`frequency_interval`,
        `contribution_recur`.`frequency_unit`,
        `contribution_recur`.`start_date`,
        `contribution_recur`.`cancel_date`,
        `contribution_recur`.`end_date`,
        `contribution_recur`.`amount`,
        `contribution_recur`.`is_test`,
        `contribution_recur`.`financial_type_id`,
        `contribution_recur`.`contribution_status_id`,
        `contribution_recur`.`currency`,
        `contribution_recur`.`campaign_id`,
        `contribution_recur`.`payment_instrument_id`,
        `contribution_recur`.`next_sched_contribution_date`
        FROM `civicrm_sdd_mandate`
        INNER JOIN `civicrm_contribution_recur` `contribution_recur` ON `contribution_recur`.`id` = `civicrm_sdd_mandate`.`entity_id` AND `civicrm_sdd_mandate`.`entity_table` = 'civicrm_contribution_recur'
        LEFT JOIN `civicrm_contribution` `first_contribution` ON `civicrm_sdd_mandate`.`first_contribution_id` = `first_contribution`.`id`
        WHERE `civicrm_sdd_mandate`.`type` = 'RCUR' 
        AND `civicrm_sdd_mandate`.`status` = %1
        AND `civicrm_sdd_mandate`.`creditor_id` = %2
        AND `contribution_recur`.`next_sched_contribution_date` <= DATE(%3)
        ORDER BY `contribution_recur`.`next_sched_contribution_date` ASC
        LIMIT %4, %5";
    $sqlParams[1] = [$mode, 'String'];
    $sqlParams[2] = [$creditor_id, 'Integer'];
    $sqlParams[3] = [$latest_date, 'String'];
    $sqlParams[4] = [$offset, 'Integer'];
    $sqlParams[5] = [$limit, 'Integer'];
    $dao = CRM_Core_DAO::executeQuery($sql, $sqlParams);
    while($dao->fetch()) {
      $mandate['id'] = $dao->id;
      $mandate['contact_id'] = $dao->contact_id;
      $mandate['contribution_recur.contact_id'] = $dao->contact_id;
      $mandate['entity_id'] = $dao->entity_id;
      $mandate['source'] = $dao->source;
      $mandate['creditor_id'] = $dao->creditor_id;
      $mandate['first_contribution.receive_date'] = $dao->receive_date;
      $mandate['contribution_recur.cycle_day'] = $dao->cycle_day;
      $mandate['contribution_recur.frequency_interval'] = $dao->frequency_interval;
      $mandate['contribution_recur.frequency_unit'] = $dao->frequency_unit;
      $mandate['contribution_recur.start_date'] = $dao->start_date;
      $mandate['contribution_recur.cancel_date'] = $dao->cancel_date;
      $mandate['contribution_recur.end_date'] = $dao->end_date;
      $mandate['contribution_recur.amount'] = $dao->amount;
      $mandate['contribution_recur.is_test'] = $dao->is_test;
      $mandate['contribution_recur.financial_type_id'] = $dao->financial_type_id;
      $mandate['contribution_recur.contribution_status_id'] = $dao->contribution_status_id;
      $mandate['contribution_recur.currency'] = $dao->currency;
      $mandate['contribution_recur.campaign_id'] = $dao->campaign_id;
      $mandate['contribution_recur.payment_instrument_id'] = $dao->payment_instrument_id;
      $mandate['contribution_recur.next_sched_contribution_date'] = $dao->next_sched_contribution_date;
      $mandate += [
        'mandate_id' => $mandate['id'],
        'mandate_contact_id' => $mandate['contact_id'],
        'mandate_entity_id' => $mandate['entity_id'],
        'mandate_first_executed' => $mandate['first_contribution.receive_date'],
        'mandate_source' => $mandate['source'],
        'mandate_creditor_id' => $mandate['creditor_id'],
        'cycle_day' => $mandate['contribution_recur.cycle_day'],
        'frequency_interval' => $mandate['contribution_recur.frequency_interval'],
        'frequency_unit' => $mandate['contribution_recur.frequency_unit'],
        'start_date' => $mandate['contribution_recur.start_date'],
        'end_date' => $mandate['contribution_recur.end_date'],
        'cancel_date' => $mandate['contribution_recur.cancel_date'],
        'rc_contact_id' => $mandate['contribution_recur.contact_id'],
        'rc_amount' => $mandate['contribution_recur.amount'],
        'rc_currency' => $mandate['contribution_recur.currency'],
        'rc_contribution_status_id' => $mandate['contribution_recur.contribution_status_id'],
        'rc_campaign_id' => $mandate['contribution_recur.campaign_id'] ?? NULL,
        'rc_payment_instrument_id' => $mandate['contribution_recur.payment_instrument_id'],
        'rc_is_test' => $mandate['contribution_recur.is_test'],
      ];

      // RCUR-STEP 2: calculate next execution date
      $next_date = $mandate['contribution_recur.next_sched_contribution_date'];
      if (NULL === $next_date || $next_date > $latest_date) {
        continue;
      }
      if (!isset($mandates_by_nextdate[$next_date])) {
        $mandates_by_nextdate[$next_date] = [];
      }
      if (!isset($mandates_by_nextdate[$next_date][$mandate['contribution_recur.financial_type_id']])) {
        $mandates_by_nextdate[$next_date][$mandate['contribution_recur.financial_type_id']] = [];
      }
      array_push($mandates_by_nextdate[$next_date][$mandate['contribution_recur.financial_type_id']], $mandate);
    }


    // RCUR-STEP 3: find already created contributions
    $existing_contributions_by_recur_id = [];
    foreach ($mandates_by_nextdate as $collection_date => $financial_type_mandates) {
      foreach ($financial_type_mandates as $financial_type => $mandates) {
        $rcontrib_ids = [];
        foreach ($mandates as $mandate) {
          array_push($rcontrib_ids, $mandate['mandate_entity_id']);
        }
        $rcontrib_id_strings = implode(',', $rcontrib_ids);

        $sql_query = "
        SELECT
          contribution.contribution_recur_id AS contribution_recur_id,
          contribution.id                    AS contribution_id
        FROM civicrm_contribution contribution
        LEFT JOIN civicrm_sdd_contribution_txgroup ctxg ON ctxg.contribution_id = contribution.id
        LEFT JOIN civicrm_sdd_txgroup               txg ON txg.id = ctxg.txgroup_id
        WHERE contribution.contribution_recur_id IN ({$rcontrib_id_strings})
          AND DATE(contribution.receive_date) = DATE('{$collection_date}')
          AND (txg.type IS NULL OR txg.type IN ('RCUR', 'FRST'))
          AND contribution.payment_instrument_id IN ({$payment_instrument_id_list});";
        $results = CRM_Core_DAO::executeQuery($sql_query);
        while ($results->fetch()) {
          $existing_contributions_by_recur_id[$results->contribution_recur_id] = $results->contribution_id;
        }
      }
    }

    // RCUR-STEP 4: create the missing contributions, store all in $mandate['mandate_entity_id']
    $count = 0;
    foreach ($mandates_by_nextdate as $collection_date => $financial_type_mandates) {
      foreach ($financial_type_mandates as $financial_type => $mandates) {
        foreach ($mandates as $index => $mandate) {
          $recur_id = $mandate['mandate_entity_id'];
          if (isset($existing_contributions_by_recur_id[$recur_id])) {
            // if the contribution already exists, store it
            $contribution_id = $existing_contributions_by_recur_id[$recur_id];
            unset($existing_contributions_by_recur_id[$recur_id]);
            $mandates_by_nextdate[$collection_date][$financial_type][$index]['mandate_entity_id'] = $contribution_id;
          }
          else {
            // else: create it
            $installment_pi = self::getInstallmentPaymentInstrument($creditor_id, $mandate['contribution_recur.payment_instrument_id'], ($mode == 'FRST'));
            $contribution_data = array(
              "total_amount"                        => $mandate['contribution_recur.amount'],
              "currency"                            => $mandate['contribution_recur.currency'],
              "receive_date"                        => $collection_date,
              "contact_id"                          => $mandate['contribution_recur.contact_id'],
              "contribution_recur_id"               => $recur_id,
              "source"                              => $mandate['mandate_source'],
              "financial_type_id"                   => $mandate['contribution_recur.financial_type_id'],
              "contribution_status_id"              => $mandate['contribution_recur.contribution_status_id'],
              "campaign_id"                         => $mandate['contribution_recur.campaign_id'],
              "is_test"                             => $mandate['contribution_recur.is_test'],
              "payment_instrument_id"               => $installment_pi
            );

            try {
            $contribution = \Civi\Api4\Contribution::create(TRUE)
              ->setValues($contribution_data)
              ->execute()
              ->first();

            //$contribution = civicrm_api('Contribution', 'create', $contribution_data);
            //if (empty($contribution['is_error'])) {
              // Success! Call the post_create hook
              CRM_Utils_SepaCustomisationHooks::installment_created($mandate['mandate_id'], $recur_id, $contribution['id']);

              // 'mandate_entity_id' will now be overwritten with the contribution instance ID
              //  to allow compatibility in with OOFF groups in the syncGroups function
              $mandates_by_nextdate[$collection_date][$financial_type][$index]['mandate_entity_id'] = $contribution['id'];
            //}
            //else {
            } catch (\Exception $e) {
              // in case of an error, we will unset 'mandate_entity_id', so it cannot be
              //  interpreted as the contribution instance ID (see above)
              unset($mandates_by_nextdate[$collection_date][$financial_type][$index]['mandate_entity_id']);

              // log the error
              Civi::log()->debug("org.project60.sepa: batching:updateRCUR/createContrib ".$e->getMessage());

              // TODO: Error handling?
            }
            unset($existing_contributions_by_recur_id[$recur_id]);
          }
          $count ++;
        }
      }
    }
    
    if ($count > 0) {
      // step 5: find all existing OPEN groups
      $sql_query = "
        SELECT
          txgroup.collection_date AS collection_date,
          txgroup.financial_type_id AS financial_type_id,
          txgroup.id AS txgroup_id
        FROM civicrm_sdd_txgroup AS txgroup
        WHERE txgroup.type = '$mode'
          AND txgroup.sdd_creditor_id = $creditor_id
          AND txgroup.status_id = $group_status_id_open;";
      $results = CRM_Core_DAO::executeQuery($sql_query);
      $existing_groups = [];
      while ($results->fetch()) {
        $collection_date = date('Y-m-d', strtotime($results->collection_date));
        $existing_groups[$collection_date][$results->financial_type_id ?? 0] = $results->txgroup_id;
      }

      // step 6: sync calculated group structure with existing (open) groups
      self::syncGroups(
        $mandates_by_nextdate,
        $existing_groups,
        $mode,
        $rcur_notice,
        $creditor_id,
        NULL !== $offset,
        0 === $offset
      );
    }
  }

  /**
   * subroutine to create the group/contribution structure as calculated
   * @param $calculated_groups  array [collection_date] -> array(contributions) as calculated
   * @param $existing_groups    array [collection_date] -> array(contributions) as currently present
   * @param $mode               SEPA mode (OOFF, RCUR, FRST)
   * @param $notice             notice days
   * @param $creditor_id        SDD creditor ID
   * @param $partial_groups     Is this a partial update?
   * @param $partial_first      Is this the first call in a partial update?
   */
  protected static function syncGroups(
    $calculated_groups,
    $existing_groups,
    $mode,
    $notice,
    $creditor_id,
    $partial_groups=FALSE,
    $partial_first=FALSE
  ) {
    $group_status_id_open = (int) CRM_Core_PseudoConstant::getKey('CRM_Batch_BAO_Batch', 'status_id', 'Open');

    foreach ($calculated_groups as $collection_date => $financial_type_groups) {
      // check if we need to defer the collection date (e.g. due to bank holidays)
      CRM_Sepa_Logic_Batching::deferCollectionDate($collection_date, $creditor_id);

      // If not using financial type grouping, flatten to a "0" financial type.
      if (!CRM_Sepa_Logic_Settings::getGenericSetting('sdd_financial_type_grouping')) {
        $financial_type_groups = [0 => array_merge(...$financial_type_groups)];
      }

      foreach ($financial_type_groups as $financial_type_id => $mandates) {
        $group_id = self::getOrCreateTransactionGroup(
          (int) $creditor_id,
          $mode,
          $collection_date,
          0 === $financial_type_id ? NULL : $financial_type_id,
          (int) $notice,
          $existing_groups
        );

        // now we have the right group. Prepare some parameters...
        $entity_ids = [];
        foreach ($mandates as $mandate) {
          // remark: "mandate_entity_id" in this case means the contribution ID
          if (empty($mandate['mandate_entity_id'])) {
            // this shouldn't happen
            Civi::log()
              ->debug("org.project60.sepa: batching:syncGroups mandate with bad mandate_entity_id ignored:" . $mandate['mandate_id']);
          }
          else {
            array_push($entity_ids, $mandate['mandate_entity_id']);
          }
        }
        if (count($entity_ids) <= 0) {
          continue;
        }

        // now, filter out the entity_ids that are are already in a non-open group
        //   (DO NOT CHANGE CLOSED GROUPS!)
        $entity_ids_list = implode(',', $entity_ids);
        $already_sent_contributions = CRM_Core_DAO::executeQuery(
          <<<SQL
              SELECT contribution_id
              FROM civicrm_sdd_contribution_txgroup
              LEFT JOIN civicrm_sdd_txgroup ON civicrm_sdd_contribution_txgroup.txgroup_id = civicrm_sdd_txgroup.id
              WHERE contribution_id IN ($entity_ids_list)
              AND  civicrm_sdd_txgroup.status_id <> $group_status_id_open;
              SQL
        );
        while ($already_sent_contributions->fetch()) {
          $index = array_search($already_sent_contributions->contribution_id, $entity_ids);
          if ($index !== FALSE) {
            unset($entity_ids[$index]);
          }
        }
        if (count($entity_ids) <= 0) {
          continue;
        }

        // remove all the unwanted entries from our group
        $entity_ids_list = implode(',', $entity_ids);
        if (!$partial_groups || $partial_first) {
          CRM_Core_DAO::executeQuery(
            <<<SQL
                DELETE FROM civicrm_sdd_contribution_txgroup
                  WHERE
                    txgroup_id=$group_id
                    AND contribution_id NOT IN ($entity_ids_list);
                SQL
          );
        }

        // remove all our entries from other groups, if necessary
        CRM_Core_DAO::executeQuery(
          <<<SQL
              DELETE FROM civicrm_sdd_contribution_txgroup
                WHERE txgroup_id!=$group_id
                AND contribution_id IN ($entity_ids_list);
              SQL
        );

        // now check which ones are already in our group...
        $existing = CRM_Core_DAO::executeQuery(
          <<<SQL
              SELECT *
              FROM civicrm_sdd_contribution_txgroup
              WHERE txgroup_id=$group_id
              AND contribution_id IN ($entity_ids_list);
              SQL
        );
        while ($existing->fetch()) {
          // remove from entity ids, if in there:
          if (($key = array_search($existing->contribution_id, $entity_ids)) !== FALSE) {
            unset($entity_ids[$key]);
          }
        }

        // the remaining must be added
        foreach ($entity_ids as $entity_id) {
          CRM_Core_DAO::executeQuery(
            <<<SQL
                INSERT INTO civicrm_sdd_contribution_txgroup (txgroup_id, contribution_id) VALUES ($group_id, $entity_id);
                SQL
          );
        }
      }
    }

    if (!$partial_groups) {
      // do some cleanup
      CRM_Sepa_Logic_Group::cleanup($mode);
    }
  }

  public static function getOrCreateTransactionGroup(
    int $creditor_id,
    string $mode,
    string $collection_date,
    ?int $financial_type_id,
    int $notice,
    array &$existing_groups
  ): int {
    $group_status_id_open = (int) CRM_Core_PseudoConstant::getKey('CRM_Batch_BAO_Batch', 'status_id', 'Open');

    if (!isset($existing_groups[$collection_date][$financial_type_id ?? 0])) {
      // this group does not yet exist -> create

      // find unused reference
      $reference = self::getTransactionGroupReference($creditor_id, $mode, $collection_date, $financial_type_id);

      $groupData = [
        'reference'               => $reference,
        'type'                    => $mode,
        'collection_date'         => $collection_date,
        // Financial type may be NULL if not grouping by financial type.
        'financial_type_id'       => $financial_type_id,
        'latest_submission_date'  => date('Y-m-d', strtotime("-$notice days", strtotime($collection_date))),
        'created_date'            => date('Y-m-d'),
        'status_id'               => $group_status_id_open,
        'sdd_creditor_id'         => $creditor_id,
      ];
      try {
        $group = Civi\Api4\SepaTransactionGroup::create(TRUE)->setValues($groupData)->execute()->first();
      } catch (\Exception $e) {
        Civi::log()->debug("org.project60.sepa: batching:syncGroups/createGroup ".$e->getMessage());
      }
    }
    else {
      $group['id'] = $existing_groups[$collection_date][$financial_type_id ?? 0];
      unset($existing_groups[$collection_date][$financial_type_id ?? 0]);
    }

    return (int) $group['id'];
  }

  /**
   * Check if a transaction group reference is already in use
   */
  public static function referenceExists($reference) {
    $sqlParams[1] = [$reference, 'String'];
    return (bool) \CRM_Core_DAO::singleValueQuery("SELECT COUNT(*) FROM `civicrm_sdd_txgroup` WHERE `reference` = %1", $sqlParams);
  }

  public static function getTransactionGroupReference(
    int $creditorId,
    string $mode,
    string $collectionDate,
    ?int $financialTypeId = NULL
  ): string {
    $defaultReference = "TXG-{$creditorId}-{$mode}-{$collectionDate}";
    if (isset($financialTypeId)) {
      $defaultReference .= "-{$financialTypeId}";
    }

    $counter = 0;
    $reference = $defaultReference;
    while (self::referenceExists($reference)) {
      $counter += 1;
      $reference = "{$defaultReference}--".$counter;
    }

    // Call the hook.
    CRM_Utils_SepaCustomisationHooks::modify_txgroup_reference(
      $reference,
      $creditorId,
      $mode,
      $collectionDate,
      $financialTypeId
    );

    return $reference;
  }

  /**
   * Determine the correct payment instrument the next installment
   *  for the given creditor/recurring contribution ID
   *
   * @param integer $creditor_id
   *   creditor ID
   * @param integer $recurring_contribution_pi
   *   recurring contribution's payment instrument
   * @param boolean $is_first
   *   is the next installment the first contribution?
   */
  public static function getInstallmentPaymentInstrument($creditor_id, $recurring_contribution_pi, $is_first)
  {
    if (!$is_first) {
      // in the RCUR case, this is simple: it's the same as the recurring contribution
      return $recurring_contribution_pi;
    }

    // OK: we're looking for the matching FRST PI for a given RCUR PI (from the recurring contribution)
    // get the creditor
    static $cache = [];
    if (isset($cache[$creditor_id][$recurring_contribution_pi])) {
      return $cache[$creditor_id][$recurring_contribution_pi];
    }

    $creditors = CRM_Sepa_Logic_PaymentInstruments::getAllSddCreditors();
    $creditor = $creditors[$creditor_id] ?? NULL;
    if (!$creditor) {
      $cache[$creditor_id][$recurring_contribution_pi] = $recurring_contribution_pi;
      return $recurring_contribution_pi; // creditor not found
    }

    // we found our creditor
    if (isset($creditor['pi_rcur'])) {
      foreach (explode(',', $creditor['pi_rcur']) as $pi_spec) {
        if (strstr($pi_spec, '-')) {
          // this is a frst-rcur combo
          $frst_rcur = explode('-', $pi_spec, 2);
          if ($frst_rcur[1] == $recurring_contribution_pi) {
            $cache[$creditor_id][$recurring_contribution_pi] = $frst_rcur[0];
            return $frst_rcur[0];
          }
        } else {
          // if this matches an individual PI, we're also happy
          if ($pi_spec == $recurring_contribution_pi) {
            $cache[$creditor_id][$recurring_contribution_pi] = $recurring_contribution_pi;
            return $recurring_contribution_pi;
          }
        }
      }
    }

    // fallback (happens e.g. if creditor settings have changed, or recurring contribution has been manipulated)
    $cache[$creditor_id][$recurring_contribution_pi] = $recurring_contribution_pi;
    return $recurring_contribution_pi;
  }

}