### Updater for HDX internal boundaries

This repo updates the subnational boundaries maintained on HDX that are used in HDX data explorers and other visuals.

### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-viz-inputs** as specified in the parameter *user_agent_lookup*.
 
Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE.

### Process

Subnational COD administrative boundaries are downloaded, international boundaries are adjusted to match the UN boundaries, and they are converted to centroid. Both polygon and centroid subnational boundaries are updated in HDX.
