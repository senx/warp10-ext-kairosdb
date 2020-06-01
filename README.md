# KairosDB WarpScript Extension

This extension allows to interact with a [KairosDB](http://kairosdb.github.io/) instance from the WarpScript language. It adds two functions, `KFETCH` and `KUPDATE` to the WarpScript language.

## Enabling the Extension

The extension has a single configuration key that can be defined. This key, named `kairosdb.validator` can be set to some WarpScript code which will be executed to validate a KairosDB endpoint. This code is called with a URL as input (a `STRING`) and is expected to return a potentially modified URL or throw an exception (via [`MSGFAIL`](https://warp10.io/doc/MSGFAIL)).

By default no modification of URLs is performed, this can cause a risk as it allows the user to indirectly issue a `POST` request to any host and port. Is this risk is a concern, please set the `kairosdb.validator` configuration to limit the accessible URLs. The following configuration will call the `kairos/validate` macro for each URL:

```
kairosdb.validator = @kairos/validate
```

The following line should be added to your Warp 10 configuration to enable the extension:

```
warpscript.extension.kairosdb = io.warp10.ext.kairosdb.KairosDBWarpScriptExtension
```

Make sure you copied the `.jar` file of the extension in the `lib` directory of your Warp 10 deployment and restart your Warp 10 instance.

## Using the extension

Please refer to the documentation of the `KFETCH` and `KUPDATE` functions.
