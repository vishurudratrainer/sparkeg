"""Generates the complete Advanced PySpark Visual Masterclass PowerPoint presentation with all modules."""

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.util import Inches, Pt

# Initialize Presentation (16:9 Widescreen)
prs = Presentation()
prs.slide_width = Inches(13.33)
prs.slide_height = Inches(7.5)
blank_layout = prs.slide_layouts[6]

# Color Palette Constants
COLOR_BG_DARK = RGBColor(20, 35, 60)  # Deep Navy
COLOR_ACCENT = RGBColor(0, 120, 215)  # Spark Blue
COLOR_TEXT_DARK = RGBColor(30, 30, 30)
COLOR_TEXT_LIGHT = RGBColor(255, 255, 255)


def add_visual_slide(title_text, core_concept, anti_pattern, golden_rule):
  slide = prs.slides.add_slide(blank_layout)

  # 1. Slide Header Background Block
  header_box = slide.shapes.add_shape(
      MSO_SHAPE.RECTANGLE, Inches(0), Inches(0), Inches(13.33), Inches(1.1)
  )
  header_box.fill.solid()
  header_box.fill.fore_color.rgb = COLOR_BG_DARK
  header_box.line.fill.background()

  # Header Title Text
  tf_title = header_box.text_frame
  tf_title.word_wrap = True
  p_title = tf_title.paragraphs[0]
  p_title.text = title_text
  p_title.font.size = Pt(22)
  p_title.font.bold = True
  p_title.font.color.rgb = COLOR_TEXT_LIGHT

  # 2. Three Visual Cards Structure
  card_data = [
      ("CORE CONCEPT", core_concept, RGBColor(220, 235, 252)),
      ("THE ANTI-PATTERN", anti_pattern, RGBColor(255, 228, 228)),
      ("GOLDEN OPTIMIZATION RULE", golden_rule, RGBColor(223, 246, 228)),
  ]

  card_width = Inches(3.7)
  card_height = Inches(5.4)
  card_top = Inches(1.5)
  left_margin = Inches(0.8)
  gap = Inches(0.4)

  for i, (heading, text, bg_color) in enumerate(card_data):
    card_left = left_margin + i * (card_width + gap)

    card = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE,
        card_left,
        card_top,
        card_width,
        card_height,
    )
    card.fill.solid()
    card.fill.fore_color.rgb = bg_color
    card.line.color.rgb = COLOR_ACCENT

    tf = card.text_frame
    tf.word_wrap = True

    p_head = tf.paragraphs[0]
    p_head.text = heading
    p_head.font.size = Pt(13)
    p_head.font.bold = True
    p_head.font.color.rgb = COLOR_BG_DARK
    p_head.space_after = Pt(10)

    p_body = tf.add_paragraph()
    p_body.text = text
    p_body.font.size = Pt(14)
    p_body.font.color.rgb = COLOR_TEXT_DARK


# Full Slide Deck Content Modules (All 13 Modules)
slides_content = [
    (
        "Module 1: Catalyst Optimizer & Physical Plans",
        (
            "Spark translates DataFrame code into a logical plan, applies"
            " optimization rules via the Catalyst Optimizer, and compiles it"
            " into a low-level physical execution plan."
        ),
        (
            "Writing redundant transformations or complex nested structures"
            " that prevent source-level filtering."
        ),
        (
            "Always inspect plans using .explain(True). Look for PushedFilters"
            " to guarantee predicate pushdown at the storage layer."
        ),
    ),
    (
        "Module 2: Strategic Caching & Memory",
        (
            "Caching intermediate DataFrames saves lineage recalculation"
            " overhead across multiple downstream actions."
        ),
        (
            "Indiscriminately caching every DataFrame, filling up executor"
            " storage memory and causing spills."
        ),
        (
            "Cache only when a dataset is reused multiple times. Always call"
            " .unpersist() when finished."
        ),
    ),
    (
        "Module 3 & 10: Conquering Data Skew & AQE",
        (
            "Data skew concentrates 90% of data onto a single partition key,"
            " leaving cluster workers idle behind a bottleneck task."
        ),
        (
            "Ignoring key distributions and relying on default partition counts"
            " for heavy join operations."
        ),
        (
            "Enable Adaptive Query Execution (spark.sql.adaptive.enabled=true)"
            " to let Spark dynamically split skewed tasks."
        ),
    ),
    (
        "Module 4: Broadcast Joins & Pandas UDFs",
        (
            "Broadcast Hash Joins eliminate network shuffles by copying small"
            " dimension tables directly to all workers."
        ),
        (
            "Using slow row-by-row Python UDFs and shuffle-heavy Sort-Merge"
            " Joins for reference tables."
        ),
        (
            "Force broadcast joins for lookup tables and replace standard"
            " Python UDFs with Arrow-backed @pandas_udf."
        ),
    ),
    (
        "Module 5: Repartition vs. Coalesce",
        (
            "repartition() redistributes data via a full network shuffle;"
            " coalesce() merges partitions locally without a network shuffle."
        ),
        (
            "Using repartition() when simply trying to reduce output file count"
            " before writing data."
        ),
        (
            "Use coalesce() locally before writing files, and repartition()"
            " when rebalancing skewed cluster tasks."
        ),
    ),
    (
        "Module 6: Optimized Window Functions",
        (
            "Window functions compute analytical metrics across sliding data"
            " partitions like running totals and moving averages."
        ),
        (
            "Defining window specifications without partition keys, funneling"
            " all records into a single global task."
        ),
        (
            "Always include .partitionBy() in your window specifications to"
            " distribute workloads evenly."
        ),
    ),
    (
        "Module 7: Small-File Compaction Strategy",
        (
            "Consolidating millions of tiny files to relieve the driver from"
            " heavy metadata tracking overhead."
        ),
        (
            "Writing thousands of micro-batches or single-row partitions"
            " directly to cloud storage layers."
        ),
        (
            "Compact small partitions using coalesce() or repartitioning"
            " strategies before writing output data."
        ),
    ),
    (
        "Module 8: Table Bucketing for Fast Joins",
        (
            "Pre-sorting and hashing tables on disk by a common join key so"
            " Spark skips network shuffles entirely."
        ),
        (
            "Shuffling two massive tables across the network every single day"
            " for recurring scheduled joins."
        ),
        (
            "Bucket heavy, frequently joined production tables on disk ahead"
            " of time to eliminate shuffle overhead."
        ),
    ),
    (
        "Module 9: Native Functions vs. Python UDFs",
        (
            "Native JVM functions execute inside optimized Tungsten binary"
            " execution engines without serialization penalties."
        ),
        (
            "Writing custom Python UDFs for basic operations like string"
            " manipulation or basic arithmetic."
        ),
        (
            "Always prioritize native built-in functions over custom Python"
            " UDFs for maximum CPU instruction throughput."
        ),
    ),
    (
        "Module 31: Lakehouse Multi-Column Clustering",
        (
            "Co-locating related data across multiple columns in storage files"
            " to maximize data skipping during file scans."
        ),
        (
            "Relying solely on single-column date partitioning when queries"
            " frequently filter by customer ID or region."
        ),
        (
            "Use lakehouse Z-Ordering (OPTIMIZE table ZORDER BY) to co-locate"
            " multi-dimensional filter attributes."
        ),
    ),
    (
        "Module 32: Preventing Broadcast Timeouts",
        (
            "Drivers take time to serialize and send broadcast payloads to"
            " executors over the network."
        ),
        (
            "Leaving spark.sql.broadcastTimeout at default 300s when"
            " broadcasting large dimension payloads."
        ),
        (
            "Increase timeout limits (e.g., 600s+) for larger broadcast tables"
            " to prevent unexpected TaskTimeout exceptions."
        ),
    ),
    (
        "Module 33: Controlling Scan File Sizes",
        (
            "Spark groups files into ~128MB partitions by default when reading"
            " data from distributed storage."
        ),
        (
            "Accepting default partition sizing when dealing with massive"
            " uncompressed raw text files."
        ),
        (
            "Tune spark.sql.files.maxPartitionBytes to smaller limits for"
            " higher parallelism on CPU-bound workloads."
        ),
    ),
    (
        "Module 34: Executor Garbage Collection (GC)",
        (
            "JVM garbage collection pauses can halt task execution during"
            " heavy object-creation workloads."
        ),
        (
            "Using default parallel GC settings that trigger frequent"
            " stop-the-world executor pauses."
        ),
        (
            "Configure executor Java options to use G1GC (-XX:+UseG1GC) for"
            " lower latency garbage collection cycles."
        ),
    ),
]

# Generate all slides
for title, concept, anti, golden in slides_content:
  add_visual_slide(title, concept, anti, golden)

output_filename = "Advanced_PySpark_Visual_Masterclass.pptx"
prs.save(output_filename)
print(f"Full presentation with {len(slides_content)} slides successfully saved as '{output_filename}'!")