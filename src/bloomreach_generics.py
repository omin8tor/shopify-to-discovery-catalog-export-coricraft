import gzip
import json
import jsonlines
import logging
from os import getenv
import shopify
import re

shopify_url = getenv("BR_SHOPIFY_URL")
api_token = getenv("BR_SHOPIFY_PAT")

api_version = "2025-04"  # Or use current version

# Construct full shop URL
full_shop_url = f"https://{shopify_url}"

# Create and activate Shopify session
session = shopify.Session(full_shop_url, api_version, api_token)
shopify.ShopifyResource.activate_session(session)

# Now you can make GraphQL calls via shopify.GraphQL()
graphql_client = shopify.GraphQL()

logger = logging.getLogger(__name__)


# TODO: transform to iteratively build file instead of in memory
def create_products(fp, pid_identifiers = None, vid_identifiers = None):
  products = []
  
  # stream over file and index each object in bulk output
  with gzip.open(fp, 'rb') as file:
    for line in file:
      products.append(create_product(json.loads(line), pid_identifiers, vid_identifiers))
  
  return products

def extract_numeric_field(field):
    return lambda x: re.sub(r"[^\d.]+", "", (json.loads(x) if isinstance(x, str) else x).get(field, "").strip())

def extract_unit_field(field):
    return lambda x: re.sub(r"[\d.\s]+", "", (json.loads(x) if isinstance(x, str) else x).get(field, "").strip())


# Process attribute mappings defined in PRODUCT_MAPPINGS:
#
# Each mapping is a list of the form:
#     [sourceKey, targetKey, preserveSource, transformFunc]
#
# - sourceKey       : Key from the Shopify product (e.g., "sp.vendor")
# - targetKey       : Key name to be used in the output (e.g., "brand")
# - preserveSource  : 
#       0 - Only targetKey is saved
#       1 - Both targetKey and sourceKey are saved
# - transformFunc   : A function applied to the value before saving (e.g., lambda x: x.strip())
#
# Example:
#     ["sp.vendor", "brand", 1, lambda x: x]
#
# Behavior:
# - Since preserveSource = 1, both the original and transformed keys are saved:
#       "sp.vendor": "xyz"
#       "brand": "xyz"
#
# - If preserveSource = 0, only the transformed key is saved:
#       "brand": "xyz"
#
# - If sourceKey and targetKey are the same:
#     The transformed value is saved under both:
#       "sp.vendor": "xyz"
#       "sp.vendor_key": "xyz"     # original value preserved explicitly
#
# Additional Notes:
# - Empty string values are skipped and not included in the output
# - Use this mapping to normalize and enrich product attributes before exporting


 
# ALL Products and Varaint Level Info

PRODUCT_VARIANT_MAPPINGS = [
    ["sv.sku", "skuid", 1, lambda x: x],
    ["sv.image", "large_image", 1, lambda x: [x["url"]] if isinstance(x, dict) else [img["url"] for img in x]],
    ["sv.image", "sv.large_image", 1, lambda x: [x["url"]] if isinstance(x, dict) else [img["url"] for img in x]],
    
    ["sv.availableForSale", "is_sellable", 1, lambda x: x],
    ["sv.inventoryQuantity", "stock_level", 1, lambda x: x],
    # ["sv.compareAtPrice", "price", 1, lambda x: x],
    # ["sv.price", "sale_price", 1, lambda x: x],
    

    ["sp.vendor", "brand", 1, lambda x: x],
    ["sp.title", "title", 1, lambda x: x],
    ["sp.tags", "tags", 1, lambda x: x.split(",") if isinstance(x, str) else x],  
    ["sp.descriptionHtml", "description", 1, lambda x: x.strip()],
    ["sp.publishedAt", "launch_date", 1, lambda x: x],
]


# ALL Products and Varaint Metafield Level Info
PRODUCT_METAFIELD_MAPPINGS = [
    ["spm.custom.tags", "keywords", 1, lambda x: list(set(x.split(",")))],
    ["svm.c_f.property", "height", 1, extract_numeric_field("Height")],
    ["svm.c_f.property", "width", 1, extract_numeric_field("Width")],
    ["svm.c_f.property", "sv.weight", 1, extract_numeric_field("Weight")],
    ["svm.c_f.property", "sv.weightunit", 1, extract_unit_field("Weight")],
    ["svm.custom.buying_category_3", "leaf_categories", 1, lambda x: [i.strip() for i in (json.loads(x) if isinstance(x, str) else x)]],
    ["svm.custom.variant_colour", "color", 1, lambda x: ", ".join(i.strip() for i in (json.loads(x) if isinstance(x, str) else x))],
    ["svm.custom.variant_colour_group", "color_group", 1, lambda x: ", ".join(i.strip() for i in (json.loads(x) if isinstance(x, str) else x))],
    ["spm.custom.legs", "legs", 1, lambda x: get_metaobject_labels_with_images( x, graphql_client)],
    ["spm.custom.material", "material", 1, lambda x: get_metaobject_labels_with_images( x, graphql_client)],
    # ["svm.custom.product_labels", "labels", 1, lambda x: get_metaobject_labels_only( x, graphql_client)],
    ["svm.custom.product_labels_values", "labels", 1, lambda x: x.split(",") if isinstance(x, str) else x],
   
]

def get_metaobject_labels_only(gid_list, graphql_client):
    if not gid_list:
        return []

    if isinstance(gid_list, str):
        gid_list = json.loads(gid_list)

    query = """
    query ($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on Metaobject {
          fields {
            key
            value
          }
        }
      }
    }
    """

    CHUNK_SIZE = 100
    label_list = []

    for i in range(0, len(gid_list), CHUNK_SIZE):
        chunk = gid_list[i:i + CHUNK_SIZE]
        response = graphql_client.execute(query, {"ids": chunk})

        try:
            result = json.loads(response)
        except json.JSONDecodeError:
            continue

        nodes = result.get("data", {}).get("nodes", [])
        for node in nodes:
            if not node:
                continue

            label = None
            for field in node.get("fields", []):
                if field.get("key") == "label":
                    label = field.get("value")
                    break

            if label:
                label_list.append(label)

    return [", ".join(label_list)] if label_list else []

def get_metaobject_labels_with_images(gid_list, graphql_client):
    if not gid_list:
        return "{}"

    if isinstance(gid_list, str):
        gid_list = json.loads(gid_list)

    query = """
    query ($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on Metaobject {
          fields {
            key
            value
            reference {
              ... on MediaImage {
                image {
                  transformedSrc(maxWidth: 100, maxHeight: 100)
                }
              }
            }
          }
        }
      }
    }
    """

    CHUNK_SIZE = 100
    enriched_items = {}

    for i in range(0, len(gid_list), CHUNK_SIZE):
        chunk = gid_list[i:i + CHUNK_SIZE]
        response = graphql_client.execute(query, {"ids": chunk})

        try:
            result = json.loads(response)
        except json.JSONDecodeError:
            continue

        nodes = result.get("data", {}).get("nodes", [])
        for node in nodes:
            if not node:
                continue

            label = None
            image_url = None

            for field in node.get("fields", []):
                if field.get("key") == "label":
                    label = field.get("value")
                ref = field.get("reference")
                if ref and ref.get("image"):
                    image_url = ref["image"].get("transformedSrc")

            if label and image_url:
                enriched_items[label] = image_url

    return json.dumps(enriched_items)


# def get_metaobject_labels_with_images(gid_list, graphql_client):
#     if not gid_list:
#         return []

#     if isinstance(gid_list, str):
#         gid_list = json.loads(gid_list)

#     query = """
#     query ($ids: [ID!]!) {
#       nodes(ids: $ids) {
#         ... on Metaobject {
#           fields {
#             key
#             value
#             reference {
#               ... on MediaImage {
#                 image {
#                   transformedSrc(maxWidth: 100, maxHeight: 100)
#                 }
#               }
#             }
#           }
#         }
#       }
#     }
#     """

#     CHUNK_SIZE = 100
#     enriched_items = []

#     for i in range(0, len(gid_list), CHUNK_SIZE):
#         chunk = gid_list[i:i + CHUNK_SIZE]
#         response = graphql_client.execute(query, {"ids": chunk})

#         try:
#             result = json.loads(response)
#         except json.JSONDecodeError:
#             continue

#         nodes = result.get("data", {}).get("nodes", [])
#         for node in nodes:
#             if not node:
#                 continue

#             label = None
#             image_url = None

#             for field in node.get("fields", []):
#                 if field.get("key") == "label":
#                     label = field.get("value")
#                 ref = field.get("reference")
#                 if ref and ref.get("image"):
#                     image_url = ref["image"].get("transformedSrc")

#             # if label and image_url:
#             #     enriched_items.append({
#             #         "name": label,
#             #         "imageUrl": image_url
#             #     })
#             if label and image_url:
#                 enriched_items.append({
#                     "name": label,
#                     "imageUrl": image_url
#                 })
#     return enriched_items 




# def get_image_urls_from_metaobjects(gid_list, graphql_client):
#     if not gid_list:
#         return []

#     query = """
#     query ($ids: [ID!]!) {
#       nodes(ids: $ids) {
#         ... on Metaobject {
#           fields {
#             key
#             value
#             reference {
#               ... on MediaImage {
#                 image {
#                   transformedSrc(maxWidth: 100, maxHeight: 100)
#                 }
#               }
#             }
#           }
#         }
#       }
#     }
#     """

#     CHUNK_SIZE = 100
#     results = []
#     label_only_list = []

#     for i in range(0, len(gid_list), CHUNK_SIZE):
#         chunk = gid_list[i:i + CHUNK_SIZE]
#         response = graphql_client.execute(query, {"ids": chunk})

#         try:
#             result = json.loads(response)
#         except json.JSONDecodeError as e:
#             raise e

#         if not isinstance(result, dict):
#             continue

#         nodes = result.get("data", {}).get("nodes", [])
#         for node in nodes:
#             if not node:
#                 continue

#             label = None
#             image_url = None

#             for field in node.get("fields", []):
#                 if field.get("key") == "label":
#                     label = field.get("value")
#                 ref = field.get("reference")
#                 if ref and ref.get("image"):
#                     image_url = ref["image"].get("transformedSrc")

#             if label:
#                 if image_url:
#                     results.append({
#                         "name": label,
#                         "imageUrl": image_url
#                     })
#                 else:
#                     label_only_list.append(label)

#     # Append label-only result as a single comma-separated string (if any)
#     if label_only_list:
#         results.append(", ".join(label_only_list))

#     return results

def get_image_urls_from_metaobjects(gid_list, graphql_client):
    if not gid_list:
        return []

    query = """
    query ($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on Metaobject {
          fields {
            key
            value
            reference {
              ... on MediaImage {
                image {
                  transformedSrc(maxWidth: 100, maxHeight: 100)
                }
              }
            }
          }
        }
      }
    }
    """

    CHUNK_SIZE = 100
    results_with_images = []
    labels_without_images = []

    for i in range(0, len(gid_list), CHUNK_SIZE):
        chunk = gid_list[i:i + CHUNK_SIZE]
        response = graphql_client.execute(query, {"ids": chunk})

        try:
            result = json.loads(response)
        except json.JSONDecodeError as e:
            raise e

        if not isinstance(result, dict):
            continue

        nodes = result.get("data", {}).get("nodes", [])
        for node in nodes:
            if not node:
                continue

            label = None
            image_url = None

            for field in node.get("fields", []):
                if field.get("key") == "label":
                    label = field.get("value")
                ref = field.get("reference")
                if ref and ref.get("image"):
                    image_url = ref["image"].get("transformedSrc")

            if label:
                if image_url:
                    results_with_images.append({
                        "name": label,
                        "imageUrl": image_url
                    })
                else:
                    labels_without_images.append(label)

    # Return appropriate format based on presence of images
    if results_with_images:
        return results_with_images
    else:
        return labels_without_images





# def get_image_urls_from_metaobjects(gid_list, graphql_client):
#     if not gid_list:
#         return []

#     query = """
#     query ($ids: [ID!]!) {
#       nodes(ids: $ids) {
#         ... on Metaobject {
#           fields {
#             key
#             value
#             reference {
#               ... on MediaImage {
#                 image {
#                   transformedSrc(maxWidth: 100, maxHeight: 100)
#                 }
#               }
#             }
#           }
#         }
#       }
#     }
#     """

#     CHUNK_SIZE = 100
#     all_image_urls = []

#     for i in range(0, len(gid_list), CHUNK_SIZE):
#         chunk = gid_list[i:i + CHUNK_SIZE]
#         response = graphql_client.execute(query, {"ids": chunk})

#         try:
#             result = json.loads(response)
#         except json.JSONDecodeError as e:
#             raise e

#         if not isinstance(result, dict):
#             continue

#         if "data" not in result or result["data"] is None:
#             continue

#         nodes = result["data"].get("nodes", [])
#         for node in nodes:
#             if not node:
#                 continue

#             for field in node.get("fields", []):
#                 ref = field.get("reference")
#                 if ref and ref.get("image"):
#                     img_url = ref["image"].get("transformedSrc")
#                     if img_url:
#                         all_image_urls.append(img_url)

#     return all_image_urls

def create_product(shopify_product, pid_identifiers = None, vid_identifiers = None):

    # elif "collections" in prop:

    # elif "metafields" in prop:
    #   for metafield in v:
    #     attributes["spm." + metafield["key"]] = metafield["value"]
    # else:
    #   attributes["sp." + prop] = v

  return {
    "id": create_id(shopify_product, identifiers=pid_identifiers), 
    "attributes": create_attributes(shopify_product, "sp"), 
    "variants": create_variants(shopify_product, identifiers=vid_identifiers)
    }


def create_id(shopify_object, identifiers = None):
  id = "NOIDENTIFIERFOUND"
  # setup default identifiers based on common Shopify patterns
  if identifiers is None:
    identifiers = ["id"]
  else:
    identifiers = identifiers.split(",")

  for identifier in identifiers:
    if identifier in shopify_object and shopify_object[identifier]:
      id = shopify_object[identifier]
      break
    elif "id" in shopify_object:
      # If `id`` isn't supplied as custom identifier, use `id` as it should always be present
      id = shopify_object["id"]

  return id


def create_variants(shopify_product, identifiers = None):
  variants = {}
  if "variants" in shopify_product and shopify_product["variants"]:
    for variant in shopify_product["variants"]:
      variant = create_variant(variant, identifiers)
      variants[variant["id"]] = {"attributes": variant["attributes"]}
  return variants


def create_variant(shopify_variant, identifiers = None):

  # attributes = {}
  # for k,v in shopify_variant.items():
  #   if "metafields" in k:
  #     for metafield in v:
  #       # each metafield added to attributes with svm. namespace to avoid collisions
  #       #   and identify it came from a shopify variant metafield
  #       attributes["svm." + metafield["key"]] = metafield["value"]
  #   else:
  #     # each variant property added as attribute with sv. namespace
  #     #   and to identify it came from a shopify variant property
  #     attributes["sv." + k] = v

  return {
    "id": create_id(shopify_variant, identifiers),
    "attributes": create_attributes(shopify_variant, "sv")
    }



def create_attributes(shopify_object, namespace):
    attributes = {}

    for k, v in shopify_object.items():
        if "variants" in k:
            continue

        if "metafields" in k:
            for metafield in v:
                attribute_name = namespace + "m." + metafield["namespace"] + "." + metafield["key"]

                value = json.loads(metafield["value"]) if "list" in metafield["type"] else metafield["value"]

                flag_found = False
                for source_key, target_key, save_original, transform in PRODUCT_METAFIELD_MAPPINGS:
                    if attribute_name == source_key:
                        try:
                            transformed = transform(value)
                            attributes[target_key] = transformed
                            if save_original:
                                if source_key != target_key:
                                    attributes[source_key] = value
                                else:
                                    attributes[source_key + "_key"] = value
                        except Exception:
                            attributes[target_key] = None
                        flag_found = True
                if not flag_found:
                    attributes[attribute_name] = value

        elif "collections" in k:
            attributes["category_paths"] = create_category_paths(v)
            # attributes["category"] = create_category_paths_details(v)

        else:
            attr_key = namespace + "." + k
            value = v

            flag_found = False
            for source_key, target_key, save_original, transform in PRODUCT_VARIANT_MAPPINGS:
                if attr_key == source_key:
                    try:
                        transformed = transform(value)
                        attributes[target_key] = transformed
                        if save_original:
                            if source_key != target_key:
                                attributes[source_key] = value
                            else:
                                attributes[source_key + "_key"] = value
                    except Exception:
                        attributes[target_key] = None
                    flag_found = True

            if not flag_found:
                attributes[attr_key] = value

    return attributes


# TODO: pass in id and name properties to override defaults
def create_category_paths(collections):
  paths = []
  for collection in collections:
    paths.append([{ "name": collection["title"], "id": collection["handle"]}]) 
    #categoryset_rhi_update
   # paths.append([{"id": collection["handle"], "name": collection["title"]}])
    
  return paths

def create_category_paths_details(collections):
  paths = []
  for collection in collections:
    paths.append(collection["title"]) 
    #categoryset_rhi_update
   # paths.append([{"id": collection["handle"], "name": collection["title"]}])
    
  return paths



def main(fp_in, fp_out, pid_props, vid_props):
  products = create_products(fp_in, pid_identifiers=pid_props, vid_identifiers=vid_props)

  with gzip.open(fp_out, 'wb') as out:
    writer = jsonlines.Writer(out)
    for object in products:
      writer.write(object)
    writer.close()


if __name__ == '__main__':
  import argparse
  from os import getenv
  
  from sys import stdout
  
  # Define logger
  loglevel = getenv('LOGLEVEL', 'INFO').upper()
  logging.basicConfig(
    stream=stdout, 
    level=loglevel,
    format="%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
  )
  
  parser = argparse.ArgumentParser(
    description="transforms Shopify aggregated products into Bloomreach Product model with no reserved attribute mappings, apart from setting product and variant identifiers. The product and variant identifiers may be specified prior to running, however, they default to `handle` for the product identifier and `sku` for the variant identifier. All other shopify properties are prefixed with a namespace to prevent collisions with any Bloomreach reserved attributes. Product properties are prefixed with `sp.`, Product metafield properties are prefixed with `spm.`, Variant properties are prefixed with `sv.`, and Variant metafield properties are prefixed with `svm.`. This output may be loaded directly into a Bloomreach Discovery catalog as is."
  )
  
  parser.add_argument(
    "--input-file",
    help="File path of Generic Products jsonl",
    type=str,
    default=getenv("BR_INPUT_FILE"),
    required=not getenv("BR_INPUT_FILE")
  )

  parser.add_argument(
    "--output-file",
    help="Filename of output jsonl file",
    type=str,
    default=getenv("BR_OUTPUT_FILE"),
    required=not getenv("BR_OUTPUT_FILE")
  )

  parser.add_argument(
    "--pid-props",
    help="Comma separated property names to use to resolve a shopify product property to Bloomreach product identifier. Usually set to the string 'handle'.",
    type=str,
    default="handle",
    required=False
  )

  parser.add_argument(
    "--vid-props",
    help="Comma separated property names to use to resolve a shopify variant property to Bloomreach variant identifier. Usually set to the string 'sku'.",
    type=str,
    default="sku",
    required=False)

  args = parser.parse_args()
  fp_in = args.input_file
  fp_out = args.output_file
  pid_props= args.pid_props
  vid_props= args.vid_props

  main(fp_in, fp_out, pid_props, vid_props)
