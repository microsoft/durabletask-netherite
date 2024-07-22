To deploy the service 

```
pwsh scripts\deploy.ps1
```

Check that it is up and running with

```
curl -s https://functionssb4.azurewebsites.net/ping -d 12
```

To generate load, run these two scripts in two separate terminals:

fanoutfanin.bash
hellosequence.bash


both of them have long breaks between active periods so that the service has some idle periods where it will scale in.



