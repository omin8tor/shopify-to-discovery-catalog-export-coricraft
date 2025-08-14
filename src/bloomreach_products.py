import logging
import gzip
import json
import jsonlines
from os import getenv

logger = logging.getLogger(__name__)



PRODUCT_MAPPINGS = [
   
]



def create_products(fp, shopify_url):
  products = []

  with gzip.open(fp, 'rb') as file:
    for line in file:
      products.append(create_product(json.loads(line), shopify_url))

  return products

def apply_mappings(attributes, mappings):
    for mapping in mappings:
        source_key, target_key, save_original_flag, transform_fn = mapping

        if source_key not in attributes:
            continue

        original_value = attributes[source_key]
        transformed_value = transform_fn(original_value)

        # Always assign transformed value
        attributes[target_key] = transformed_value

        # Save original if flag is 1
        if save_original_flag == 1:
            if source_key != target_key:
                attributes[source_key] = original_value
            else:
                attributes[source_key + "_key"] = original_value
                

def create_product(product, shopify_url):

  out_product = {
    "id": product["id"],
    "attributes": product["attributes"].copy(),
    "variants": {}
  }

  # container for input product attributes
  in_pa = product["attributes"]
  
  # container for the transformed product attributes and variants
  # these dictionaries will be merged into out_product at the end
  out_pa = out_product["attributes"]

  out_pa["url"] = f"https://{shopify_url}/products/" + in_pa["sp.handle"]


   labels_list = []
    for key in ["spvm.custom.additional_label", "spvm.custom.product_labels_values"]:
        val = in_pa.get(key)
        if isinstance(val, str):
            labels_list.extend(v.strip() for v in val.split(",") if v.strip())
        elif isinstance(val, list):
            labels_list.extend(v for v in val if v)

    if labels_list:
        # remove duplicates while preserving order
        seen = set()
        labels_list = [x for x in labels_list if not (x in seen or seen.add(x))]
        out_pa["labels_new"] = labels_list


   

  # if in_pa["sp.status"] == "ACTIVE" and "sp.totalInventory" in in_pa and in_pa["sp.totalInventory"] > 0:
  #   out_pa["availability"] = True
  # else:
  #   out_pa["availability"] = False

  # set thumb_image from featured image (alternatively, this could be large_image)
  # https://shopify.dev/api/admin-graphql/2023-01/objects/product#field-product-featuredimage
  if "sp.featuredImage" in in_pa and in_pa["sp.featuredImage"] and "url" in in_pa["sp.featuredImage"]:
      out_pa["thumb_image"] = in_pa["sp.featuredImage"]["url"]

  apply_mappings(out_pa, PRODUCT_MAPPINGS)

  # iterate over each variant
  for v_id, variant in product["variants"].items():

    # container of input and output variant attributes
    in_va = variant["attributes"]
    out_va = variant["attributes"].copy()
    out_product["variants"][v_id] = {}

    if "sv.compareAtPrice" in in_va and in_va["sv.compareAtPrice"]:
      if in_va["sv.compareAtPrice"] == in_va["sv.price"]:
        out_va["price"] = in_va["sv.compareAtPrice"]
      else:
        out_va["price"] = in_va["sv.compareAtPrice"]
        out_va["sale_price"] = in_va["sv.price"]
    else:
      out_va["price"] = in_va["sv.price"]

    # set color, size from selectedOptions
    # TODO: set other options to custom attributes
    if "sv.selectedOptions" in in_va:
      if in_va["sv.selectedOptions"] and len(in_va["sv.selectedOptions"]) > 0:
        for option in in_va["sv.selectedOptions"]:
          if "name" in option and "value" in option and "Color" in option[
              "name"]:
            out_va["color"] = option["value"]
            # br_va["variants_color"] = option["value"]
          if "name" in option and "value" in option and "Size" in option[
              "name"]:
            out_va["size"] = option["value"]

    out_va["availability"] = False
    if "sv.availableForSale" in in_va and in_va["sv.availableForSale"]:
      out_va["availability"] = True
    else:
      out_va["availability"] = True
      out_va["sv.availableForSale"] = False

    # set thumb_image (swatch_image isn't a standard shopify concept as far as I can tell)
    # https://shopify.dev/api/admin-graphql/2023-01/objects/ProductVariant#field-productvariant-image
    if "sv.image" in in_va and in_va["sv.image"] and "url" in in_va["sv.image"]:
      out_va["thumb_image"] = in_va["sv.image"]["url"]
    
    out_product["variants"][v_id]["attributes"] = out_va

  return out_product


def main(fp_in, fp_out, shopify_url):
  patch = create_products(fp_in, shopify_url)

  # write JSONLines
  with gzip.open(fp_out, "wb") as file:
    writer = jsonlines.Writer(file)
    for object in patch:
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
    description="Transforms generic products with custom logic specific to an individual catalog. This is more or less a place holder script to add any transformations necessary that need to be made on top of the generic product transforms. For instance, if shopify product tags are used in a special way, custom transforms can be created. Also, generic transforms can be overriden should it be necessary for a catalog specific behavior. The values of the shopify prefixed attributes should not be modified."
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
    "--shopify-url",
    help="Hostname of the shopify Shop, e.g. xyz.myshopify.com.",
    type=str,
    default=getenv("BR_SHOPIFY_URL"),
    required=not getenv("BR_SHOPIFY_URL")
  )

  args = parser.parse_args()
  fp_in = args.input_file
  fp_out = args.output_file
  shopify_url = args.shopify_url

  main(fp_in, fp_out, shopify_url)
