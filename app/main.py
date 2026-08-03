from flask import Flask, render_template, request, jsonify
import pandas as pd
import os
import json
import sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

app = Flask(__name__)

# ==========================================
# SECTION 1: Data Loading & Memory Caching
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RULES_PATH = os.path.join(BASE_DIR, 'rules.csv')
MAPPING_PATH = os.path.join(BASE_DIR, 'group_mapping.json')

# Initialize global variables
rules_df = None
store_products = []
item_to_rep_map = {}

def load_data():
    global rules_df, store_products, item_to_rep_map
    try:
        # 1. Load the FP-Growth Rules
        rules_df = pd.read_csv(RULES_PATH)
        
        # 2. Load the NLP Mapping Dictionary 
        # (Maps 50k specific names to ~200 Representative names)
        with open(MAPPING_PATH, 'r') as f:
            item_to_rep_map = json.load(f)
            
        # 3. Populate the store catalog dynamically.
        # We extract only the unique Representative items that actually have rules.
        valid_items = pd.concat([rules_df['antecedents'], rules_df['consequents']]).dropna().unique()
        
        # 4. Sort by popularity (items that trigger the most rules appear first)
        popularity_counts = rules_df['antecedents'].value_counts()
        sorted_products = popularity_counts.index.tolist()
        
        # Add any remaining valid items that only appear as consequents
        for item in valid_items:
            if item not in sorted_products:
                sorted_products.append(item)
                
        store_products = sorted_products
        
        print(f"✅ Successfully loaded {len(rules_df)} association rules.")
        print(f"✅ Store populated with {len(store_products)} clustered products.")
        
    except Exception as e:
        print(f"❌ ERROR Loading Data: {e}")
        # Create an empty dataframe to prevent the app from crashing on startup
        rules_df = pd.DataFrame(columns=['antecedents', 'consequents', 'support', 'confidence', 'lift'])

# Boot sequence
load_data()


# ==========================================
# SECTION 2: Web Routes (Frontend)
# ==========================================
@app.route('/')
def home():
    """Serves the main HTML page."""
    return render_template('index.html')


# ==========================================
# SECTION 3: API Routes (Backend / AJAX)
# ==========================================
@app.route('/api/products', methods=['GET'])
def get_products():
    """Returns the list of available representative products sorted by popularity."""
    return jsonify({
        "status": "success",
        "total": len(store_products),
        "products": store_products
    })

@app.route('/api/recommend', methods=['GET'])
def get_recommendations():
    """Gets recommendations for a SINGLE item (for the sub-row dropdown)."""
    target_item = request.args.get('item')
    if not target_item:
        return jsonify({"error": "No item provided"}), 400

    # TRANSLATION LAYER: Convert the clicked item to its NLP representative cluster
    rep_item = item_to_rep_map.get(target_item, target_item)

    # Search rules using the representative cluster
    recommendations = rules_df[rules_df['antecedents'] == rep_item]
    
    if recommendations.empty:
        return jsonify({"item": target_item, "recommendations": []})
    
    # Sort by relationship strength
    recommendations = recommendations.sort_values(by='lift', ascending=False)
    
    # BULLETPROOF DEDUPLICATION: Maintain Lift order, but enforce uniqueness
    unique_top_items = []
    for consequent in recommendations['consequents']:
        # Ensure it's not a duplicate, and ensure it isn't recommending itself
        if consequent != rep_item and consequent not in unique_top_items:
            unique_top_items.append(consequent)
        
        # Stop once we have exactly 4 unique items
        if len(unique_top_items) == 4:
            break
            
    return jsonify({"item": target_item, "recommendations": unique_top_items})

@app.route('/api/cart-recommend', methods=['POST'])
def get_cart_recommendations():
    """Evaluates the entire cart and returns global recommendations."""
    data = request.json
    cart_items = data.get('cart', [])
    
    if not cart_items:
        return jsonify({"recommendations": []})

    # TRANSLATION LAYER: Convert entire cart array to representative clusters
    rep_cart_items = [item_to_rep_map.get(item, item) for item in cart_items]

    # 1. Find all rules where the antecedent is ANY item currently in the cart
    matches = rules_df[rules_df['antecedents'].isin(rep_cart_items)]
    
    # 2. Filter out consequents that the user already has in their cart
    matches = matches[~matches['consequents'].isin(rep_cart_items)]
    
    if matches.empty:
        return jsonify({"recommendations": []})
    
    # 3. Group by the recommended item and take the highest lift score
    # (e.g., if Lemons and Avocados BOTH recommend Limes, Limes gets pushed to the top)
    grouped = matches.groupby('consequents')['lift'].max().reset_index()
    top_global_recs = grouped.sort_values(by='lift', ascending=False).head(5)['consequents'].tolist()
    
    return jsonify({"recommendations": top_global_recs})


# ==========================================
# SECTION 4: Application Runner
# ==========================================
if __name__ == '__main__':
    app.run(debug=True, port=5000)