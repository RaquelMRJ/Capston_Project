CREATE TABLE IF NOT EXISTS fact_movie_analytics (
           customerid INTEGER,
           id_dim_devices INTEGER,
           id_dim_location INTEGER,
           id_dim_os INTEGER,
           id_dim_browser INTEGER,
           amount_spent DECIMAL(18, 5),
           review_score INTEGER,
           review_count INTEGER,
           insert_date DATE);

CREATE TABLE IF NOT EXISTS dim_date ( 
            id_dim_date INTEGER,
            log_date DATE,
            day VARCHAR, 
            month VARCHAR, 
            year VARCHAR, 
            season VARCHAR);

CREATE TABLE IF NOT EXISTS dim_devices ( 
            id_dim_devices INTEGER, 
            device VARCHAR);

CREATE TABLE IF NOT EXISTS dim_location ( 
            id_dim_location INTEGER, 
            location VARCHAR);

CREATE TABLE IF NOT EXISTS dim_os ( 
            id_dim_devices INTEGER, 
            os VARCHAR);

CREATE TABLE IF NOT EXISTS dim_browser ( 
            id_dim_devices INTEGER, 
            browser VARCHAR);

