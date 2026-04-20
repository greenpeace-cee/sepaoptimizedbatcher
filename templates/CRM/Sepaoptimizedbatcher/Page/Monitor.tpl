<div class="crm-block crm-form-block crm-queue-runner-form-block">
  <div class="description">{$description}</div>
  <div id="crm-queue-runner-progress"></div>
</div>

{literal}
<script type="text/javascript">

CRM.$(function($) {
  // Note: Queue API provides "#remaining tasks" but not "#completed tasks" or "#total tasks".
  // To compute a %complete, we manually track #completed. This only works nicely if we
  // assume that the queue began with a fixed #tasks.

  $("#crm-queue-runner-progress").progressbar({ value: {/literal}{$pct}{literal} });

  var displayResponseData = function(data, textStatus, jqXHR) {
    if (data.redirect_url) {
      window.location.href = data.redirect_url;
      return;
    }

    $("#crm-queue-runner-progress").progressbar({ value: data.pct });
    if (pct < 100) {
      window.setTimeout(runNext, 500);
    }
  };

  var runNext = function() {
    $.ajax({
      type: 'POST',
      url: '{/literal}{$url}{literal}',
      dataType: 'json',
      beforeSend: function(jqXHR, settings) {
          $("#crm-queue-runner-buttonset").hide();
      },
      success: function(data, textStatus, jqXHR) {
        if (data.redirect_url) {
          window.location.href = data.redirect_url;
          return;
        }

        $("#crm-queue-runner-progress").progressbar({ value: data.pct });
        window.setTimeout(runNext, 500);
      }
    });
  }

  window.setTimeout(runNext, 50);
});

</script>
{/literal}