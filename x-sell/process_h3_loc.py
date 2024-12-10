import h3
def lat_lng_to_h3(latitudes, longitudes, resolution):
    return np.vectorize(h3.geo_to_h3)(latitudes, longitudes, resolution)

def process_h3_loc(df):
    df['trip_cnts'] = df['trip_cnts'].astype(int)
    df['latitude'] = pd.to_numeric(df['latitude'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    df['customer_id'] = pd.to_numeric(df['customer_id'])

    # Convert latitude and longitude to H3 index using vectorized operations
    df['drop_off_h3_6'] = lat_lng_to_h3(df['latitude'].values, df['longitude'].values, 6)

    df_gr = df.groupby(['customer_id', 'drop_off_h3_6']).agg(trip_loc_cnts=('trip_cnts', 'sum')).reset_index()

    df_gr = df_gr.loc[df_gr.groupby('customer_id')['trip_loc_cnts'].idxmax()]

    return df_gr

def create_area_features(df, bl):
    # Function to calculate Wilson Lower Bound
    def wilson_lower_bound(successes, n, confidence=0.95):
        if n == 0:
            return 0
        z = norm.ppf(1 - (1 - confidence) / 2)
        phat = successes / n
        numerator = phat + (z**2) / (2 * n) - z * np.sqrt((phat * (1 - phat) + z**2 / (4 * n)) / n)
        denominator = 1 + z**2 / n
        return numerator / denominator
    
    # Convert the 'date' column to datetime
    df[f'{bl}_first_order_date'] = pd.to_datetime(df[f'{bl}_first_order_date'])

    # Convert the boundary dates to datetime
    start_date = pd.to_datetime(source_start_date)
    end_date = pd.to_datetime(anchor_date)
    
    df[f'{bl}_flag'] = df[f'{bl}_first_order_date'].between(start_date, end_date)

    # df[f'{bl}_first_order_date'] = df[f'{bl}_first_order_date'].dt.strftime('%Y-%m-%d')

    # Group by 'area_id' and calculate total orders and successes
    area_stats_drop = df.groupby('drop_off_h3_6_dropoff')[f'{bl}_flag'].agg(all_drop_off='size', crossed_drop_off='sum').reset_index()

    # Calculate Wilson lower bound for each area
    area_stats_drop[f'wilson_cross_percentage_drop_{bl}'] = area_stats_drop.apply(
        lambda row: wilson_lower_bound(row['crossed_drop_off'], row['all_drop_off']), axis=1
    )
    area_stats_drop = area_stats_drop[area_stats_drop['all_drop_off'] > 10]

    # Group by 'area_id' and calculate total orders and successes
    area_stats_pick = df.groupby('drop_off_h3_6_pickup')[f'{bl}_flag'].agg(all_pick_up='size', crossed_pick_up='sum').reset_index()

    # Calculate Wilson lower bound for each area
    area_stats_pick[f'wilson_cross_percentage_pick_{bl}'] = area_stats_pick.apply(
        lambda row: wilson_lower_bound(row['crossed_pick_up'], row['all_pick_up']), axis=1
    )
    area_stats_pick = area_stats_pick[area_stats_pick['all_pick_up'] > 10]

    # Merge back with original DataFrame if needed
    # df = pd.merge(df, area_stats_drop, on='drop_off_h3_6_dropoff', how='left')
    # df = pd.merge(df, area_stats_pick, on='drop_off_h3_6_pickup', how='left')

    area_stats_drop.to_parquet(f"data/{bl}_area_stats_drop_{anchor_date}.parquet", index=False)
    area_stats_pick.to_parquet(f"data/{bl}_area_stats_pick_{anchor_date}.parquet", index=False)

    # return area_stats_drop, area_stats_pick