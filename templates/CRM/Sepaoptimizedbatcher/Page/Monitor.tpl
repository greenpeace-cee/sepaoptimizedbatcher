<div class="crm-block crm-form-block crm-queue-runner-form-block" style="z-index:-99;">
  <div class="description">{$description}</div>
  <div id="crm-queue-runner-progress"></div>
</div>

{literal}
<script type="text/javascript">

CRM.$(function($) {
  $("#crm-queue-runner-progress").progressbar({ value: {/literal}{$pct}{literal} });

  var runNext = function() {
    $.ajax({
      type: 'POST',
      url: '{/literal}{$url}{literal}',
      dataType: 'json',
      /*beforeSend: function(jqXHR, settings) {
          $("#crm-queue-runner-buttonset").hide();
      },*/
      success: function(data, textStatus, jqXHR) {
        if (data.redirect_url) {
          window.location.href = data.redirect_url;
          return;
        }

        $("#crm-queue-runner-progress").progressbar( "value", data.pct );
        window.setTimeout(runNext, 500);
      }
    });
  }

  window.setTimeout(runNext, 500);
});

</script>
{/literal}