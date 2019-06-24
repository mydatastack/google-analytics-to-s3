# Google Analytics Duplicator
You can automatically duplicate Google Analytics hits to Pipes. You can do it with the help of Google Tag Manager. Below you'll find a step by step instruction. 

---
1. Step: Create a **Custom JavaScript variable** in Google Tag Manager. Call the variable `Pipes duplicator` and add the following code:

```js
function() {
  // Add your pipes collector endpoint here
  var endpoint = 'https://collector.endpoint.com/';
  
  return function(model) {
    var vendor = 'com.google.analytics';
    var version = 'v1';
    var path = ((endpoint.substr(-1) !== '/') ? endpoint + '/' : endpoint) + vendor + '/' + version;
    
    var globalSendTaskName = '_' + model.get('trackingId') + '_sendHitTask';
    
    var originalSendHitTask = window[globalSendTaskName] = window[globalSendTaskName] || model.get('sendHitTask');
    
    model.set('sendHitTask', function(sendModel) {
      var payload = sendModel.get('hitPayload');
      originalSendHitTask(sendModel);
      var request = new XMLHttpRequest();
      request.open('POST', path, true);
      request.setRequestHeader('Content-type', 'text/plain; charset=UTF-8');
      request.send(payload);
    });
  };
}
```

**Important:** Don't forget to change the endpoint in the code. The endpoint
you can find in Cloudformation under the deployed Google Analytics Nested Stack
[more](#google-analytics-duplicator-endpoint)

---

2. Step: Edit **EVERY SINGLE** Google Analytics tag whose data you want to send to Pipes. Go to **Tags**, click on a **Tag Name** you want to edit. Click on **Enable overriding settings in this tag**. Click on **+Add Field**, use `customTask` as a field name and `{{Pipes duplicator}}` as a value. Click save.

![gtm pipes](./img/gtm-pipes.png)

---

3. Step: Publish a new version in Google Tag Manager. After publishing a new version, all Google Analytics hits will be sent to Pipes automatically.

---

### Additional Information
#### Google Analytics Duplicator Endpoint

![duplicator](./img/cf-endpoint.png)

[Source ~ Simo Ahava](https://www.simoahava.com)
