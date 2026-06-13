fn open_csv_append(path: &Path, expected_header: &str) -> Result<fs::File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(fs_error)?;
    }
    let write_header = match fs::File::open(path) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            let mut first_line = String::new();
            let bytes = reader.read_line(&mut first_line).map_err(fs_error)?;
            if bytes == 0 {
                true
            } else {
                let existing_header = first_line.trim_end_matches(['\r', '\n']);
                if existing_header != expected_header {
                    return Err(StorageError::invalid_argument(format!(
                        "CSV header mismatch for {}; expected `{expected_header}`, found `{existing_header}`; use a fresh output file",
                        path.display()
                    )));
                }
                false
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => return Err(fs_error(error)),
    };
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(fs_error)?;
    if write_header {
        writeln!(file, "{expected_header}").map_err(fs_error)?;
    }
    Ok(file)
}

fn append_matrix_csv(args: &Args, report: &BenchReport) -> Result<()> {
    let Some(path) = &args.matrix_csv else {
        return Ok(());
    };
    let mut file = open_csv_append(path, BenchReport::csv_header())?;
    writeln!(file, "{}", report.csv_row()).map_err(fs_error)?;
    Ok(())
}

fn append_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.durable_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_persist_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,sequence,total_nanos,persist_lock_wait_nanos,block_delta_prestage_wait_nanos,block_delta_selected_count,block_delta_selected_bytes,native_file_delta_selected_count,native_file_delta_selected_bytes,stream_prefix_request_count,stream_prefix_plan_count,stream_prefix_record_count,stream_prefix_payload_bytes,stream_prefix_storage_node_count,stream_prefix_pending_lock_wait_nanos,sqlite_lock_wait_nanos,local_snapshot_nanos,metadata_publish_lock_wait_nanos,commit_sequence_alloc_nanos,data_log_append_sync_nanos,data_log_encode_nanos,data_log_write_nanos,data_log_file_sync_nanos,data_log_file_sync_sum_nanos,data_log_file_sync_max_nanos,data_log_files_synced,data_log_sync_bytes,data_log_records_written,data_log_write_bytes,data_log_prestaged_segment_count,data_log_prestaged_segment_bytes,data_log_sync_only_bytes,data_log_flush_write_bytes,data_log_sync_storage_node_count,data_log_dir_sync_nanos,node_catalog_publish_nanos,node_catalog_manifest_lock_wait_nanos,node_catalog_manifest_row_sync_nanos,node_catalog_manifest_commit_nanos,node_catalog_segment_lock_wait_nanos,node_catalog_segment_row_sync_nanos,node_catalog_segment_commit_nanos,node_catalog_manifest_rows,node_catalog_sealed_rows,node_catalog_placement_rows,node_catalog_segment_rows,root_sqlite_row_sync_nanos,root_sqlite_commit_nanos,visible_metadata_write_bytes,append_visible_publish_batch_id,append_visible_journal_lock_wait_nanos,append_visible_journal_encode_nanos,append_visible_journal_open_nanos,append_visible_journal_write_nanos,append_visible_journal_sync_nanos,append_visible_journal_dir_sync_nanos,append_visible_journal_record_count,append_visible_journal_frame_bytes,append_visible_journal_created,block_journal_encode_nanos,block_journal_open_nanos,block_journal_write_nanos,block_journal_sync_nanos,block_journal_dir_sync_nanos,block_journal_record_count,block_journal_frame_bytes,block_journal_created,block_journal_flush_group_size,block_journal_lane_wait_nanos,block_journal_payload_recheck_nanos,block_journal_publish_nanos,block_journal_publish_mark_nanos,block_journal_publish_reserve_nanos,block_journal_publish_apply_nanos,block_journal_publish_receipt_nanos,block_journal_publish_evidence_nanos,block_journal_publish_dispatch_nanos,block_journal_publish_verify_nanos,block_journal_publish_mark_catalog_nanos,block_journal_publish_mark_lock_wait_nanos,block_journal_overlay_read_nanos,new_segment_count,new_segment_bytes,touched_node_count,logical_conflict_count,touched_shard_head_rows,touched_manifest_rows,commit_rows_written,durable_commit_high_water";
    let mut file = open_csv_append(path, header)?;
    for profile in profiles {
        let row = [
            workload.name().to_string(),
            args.provider.to_string(),
            args.durability.to_string(),
            args.rtt.as_micros().to_string(),
            args.serial_rtts.to_string(),
            concurrency.to_string(),
            workload.op_size(args)?.to_string(),
            profile.sequence.to_string(),
            profile.total_nanos.to_string(),
            profile.lock_wait_nanos.to_string(),
            profile.block_delta_prestage_wait_nanos.to_string(),
            profile.block_delta_selected_count.to_string(),
            profile.block_delta_selected_bytes.to_string(),
            profile.native_file_delta_selected_count.to_string(),
            profile.native_file_delta_selected_bytes.to_string(),
            profile.stream_prefix_request_count.to_string(),
            profile.stream_prefix_plan_count.to_string(),
            profile.stream_prefix_record_count.to_string(),
            profile.stream_prefix_payload_bytes.to_string(),
            profile.stream_prefix_storage_node_count.to_string(),
            profile.stream_prefix_pending_lock_wait_nanos.to_string(),
            profile.sqlite_lock_wait_nanos.to_string(),
            profile.local_snapshot_nanos.to_string(),
            profile.metadata_publish_lock_wait_nanos.to_string(),
            profile.commit_sequence_alloc_nanos.to_string(),
            profile.data_log_append_sync_nanos.to_string(),
            profile.data_log_encode_nanos.to_string(),
            profile.data_log_write_nanos.to_string(),
            profile.data_log_file_sync_nanos.to_string(),
            profile.data_log_file_sync_sum_nanos.to_string(),
            profile.data_log_file_sync_max_nanos.to_string(),
            profile.data_log_files_synced.to_string(),
            profile.data_log_sync_bytes.to_string(),
            profile.data_log_records_written.to_string(),
            profile.data_log_write_bytes.to_string(),
            profile.data_log_prestaged_segment_count.to_string(),
            profile.data_log_prestaged_segment_bytes.to_string(),
            profile.data_log_sync_only_bytes.to_string(),
            profile.data_log_flush_write_bytes.to_string(),
            profile.data_log_sync_storage_node_count.to_string(),
            profile.data_log_dir_sync_nanos.to_string(),
            profile.node_catalog_publish_nanos.to_string(),
            profile.node_catalog_manifest_lock_wait_nanos.to_string(),
            profile.node_catalog_manifest_row_sync_nanos.to_string(),
            profile.node_catalog_manifest_commit_nanos.to_string(),
            profile.node_catalog_segment_lock_wait_nanos.to_string(),
            profile.node_catalog_segment_row_sync_nanos.to_string(),
            profile.node_catalog_segment_commit_nanos.to_string(),
            profile.node_catalog_manifest_rows.to_string(),
            profile.node_catalog_sealed_rows.to_string(),
            profile.node_catalog_placement_rows.to_string(),
            profile.node_catalog_segment_rows.to_string(),
            profile.root_sqlite_row_sync_nanos.to_string(),
            profile.root_sqlite_commit_nanos.to_string(),
            profile.visible_metadata_write_bytes.to_string(),
            profile.append_visible_publish_batch_id.to_string(),
            profile.append_visible_journal_lock_wait_nanos.to_string(),
            profile.append_visible_journal_encode_nanos.to_string(),
            profile.append_visible_journal_open_nanos.to_string(),
            profile.append_visible_journal_write_nanos.to_string(),
            profile.append_visible_journal_sync_nanos.to_string(),
            profile.append_visible_journal_dir_sync_nanos.to_string(),
            profile.append_visible_journal_record_count.to_string(),
            profile.append_visible_journal_frame_bytes.to_string(),
            profile.append_visible_journal_created.to_string(),
            profile.block_journal_encode_nanos.to_string(),
            profile.block_journal_open_nanos.to_string(),
            profile.block_journal_write_nanos.to_string(),
            profile.block_journal_sync_nanos.to_string(),
            profile.block_journal_dir_sync_nanos.to_string(),
            profile.block_journal_record_count.to_string(),
            profile.block_journal_frame_bytes.to_string(),
            profile.block_journal_created.to_string(),
            profile.block_journal_flush_group_size.to_string(),
            profile.block_journal_lane_wait_nanos.to_string(),
            profile.block_journal_payload_recheck_nanos.to_string(),
            profile.block_journal_publish_nanos.to_string(),
            profile.block_journal_publish_mark_nanos.to_string(),
            profile.block_journal_publish_reserve_nanos.to_string(),
            profile.block_journal_publish_apply_nanos.to_string(),
            profile.block_journal_publish_receipt_nanos.to_string(),
            profile.block_journal_publish_evidence_nanos.to_string(),
            profile.block_journal_publish_dispatch_nanos.to_string(),
            profile.block_journal_publish_verify_nanos.to_string(),
            profile.block_journal_publish_mark_catalog_nanos.to_string(),
            profile.block_journal_publish_mark_lock_wait_nanos.to_string(),
            profile.block_journal_overlay_read_nanos.to_string(),
            profile.new_segment_count.to_string(),
            profile.new_segment_bytes.to_string(),
            profile.touched_node_count.to_string(),
            profile.logical_conflict_count.to_string(),
            profile.touched_shard_head_rows.to_string(),
            profile.touched_manifest_rows.to_string(),
            profile.commit_rows_written.to_string(),
            profile.durable_commit_high_water.to_string(),
        ];
        writeln!(file, "{}", row.join(","))
        .map_err(fs_error)?;
    }
    Ok(())
}

fn append_append_publish_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.append_publish_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_append_publish_wait_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,sequence,ticket_id,stream_id,publish_through,total_nanos,status_check_nanos,coordinator_lock_wait_nanos,coordinator_wait_nanos,in_flight_wait_nanos,coalesce_wait_nanos,persist_batch_nanos,persist_batch_metadata_gate_wait_nanos,persist_batch_plan_nanos,persist_batch_durable_nanos,persist_batch_apply_nanos,compact_delta_drain_nanos,compact_delta_drain_attempts,compact_delta_drain_successes,full_persist_nanos,full_persist_count,wait_loops,cvar_waits,in_flight_waits,in_flight_batches_waited,coalesce_waits,persist_batches_started,max_batch_ticket_count,batch_waiter_request_count,batch_metadata_pending_ticket_count,batch_coalesce_start_demand,batch_coalesce_end_demand,batch_coalesce_hit_target,batch_planned_ticket_count,batch_completed_ticket_count,batch_same_file_skip_count,batch_journal_lane_count,batch_shared_journal,post_batch_request_count,post_batch_pending_ticket_count,append_publish_batch_id,payload_already_durable_bytes,payload_synced_bytes,payload_sync_nanos,visible_metadata_commit_nanos,catalog_manifest_publish_nanos,append_visible_journal_lock_wait_nanos,append_visible_journal_encode_nanos,append_visible_journal_open_nanos,append_visible_journal_write_nanos,append_visible_journal_sync_nanos,append_visible_journal_dir_sync_nanos,append_visible_journal_record_count,append_visible_journal_frame_bytes,append_visible_journal_created,registered,completed_without_register,success";
    let mut file = open_csv_append(path, header)?;
    for profile in profiles {
        let row = [
            workload.name().to_string(),
            args.provider.to_string(),
            args.durability.to_string(),
            args.rtt.as_micros().to_string(),
            args.serial_rtts.to_string(),
            concurrency.to_string(),
            workload.op_size(args)?.to_string(),
            profile.sequence.to_string(),
            profile.ticket_id.to_string(),
            profile.stream_id.to_string(),
            profile.publish_through.to_string(),
            profile.total_nanos.to_string(),
            profile.status_check_nanos.to_string(),
            profile.coordinator_lock_wait_nanos.to_string(),
            profile.coordinator_wait_nanos.to_string(),
            profile.in_flight_wait_nanos.to_string(),
            profile.coalesce_wait_nanos.to_string(),
            profile.persist_batch_nanos.to_string(),
            profile.persist_batch_metadata_gate_wait_nanos.to_string(),
            profile.persist_batch_plan_nanos.to_string(),
            profile.persist_batch_durable_nanos.to_string(),
            profile.persist_batch_apply_nanos.to_string(),
            profile.compact_delta_drain_nanos.to_string(),
            profile.compact_delta_drain_attempts.to_string(),
            profile.compact_delta_drain_successes.to_string(),
            profile.full_persist_nanos.to_string(),
            profile.full_persist_count.to_string(),
            profile.wait_loops.to_string(),
            profile.cvar_waits.to_string(),
            profile.in_flight_waits.to_string(),
            profile.in_flight_batches_waited.to_string(),
            profile.coalesce_waits.to_string(),
            profile.persist_batches_started.to_string(),
            profile.max_batch_ticket_count.to_string(),
            profile.batch_waiter_request_count.to_string(),
            profile.batch_metadata_pending_ticket_count.to_string(),
            profile.batch_coalesce_start_demand.to_string(),
            profile.batch_coalesce_end_demand.to_string(),
            profile.batch_coalesce_hit_target.to_string(),
            profile.batch_planned_ticket_count.to_string(),
            profile.batch_completed_ticket_count.to_string(),
            profile.batch_same_file_skip_count.to_string(),
            profile.batch_journal_lane_count.to_string(),
            profile.batch_shared_journal.to_string(),
            profile.post_batch_request_count.to_string(),
            profile.post_batch_pending_ticket_count.to_string(),
            profile.append_publish_batch_id.to_string(),
            profile.payload_already_durable_bytes.to_string(),
            profile.payload_synced_bytes.to_string(),
            profile.payload_sync_nanos.to_string(),
            profile.visible_metadata_commit_nanos.to_string(),
            profile.catalog_manifest_publish_nanos.to_string(),
            profile.append_visible_journal_lock_wait_nanos.to_string(),
            profile.append_visible_journal_encode_nanos.to_string(),
            profile.append_visible_journal_open_nanos.to_string(),
            profile.append_visible_journal_write_nanos.to_string(),
            profile.append_visible_journal_sync_nanos.to_string(),
            profile.append_visible_journal_dir_sync_nanos.to_string(),
            profile.append_visible_journal_record_count.to_string(),
            profile.append_visible_journal_frame_bytes.to_string(),
            profile.append_visible_journal_created.to_string(),
            profile.registered.to_string(),
            profile.completed_without_register.to_string(),
            profile.success.to_string(),
        ];
        writeln!(file, "{}", row.join(",")).map_err(fs_error)?;
    }
    Ok(())
}

fn append_append_ingest_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.append_ingest_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_append_ingest_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,sequence,stream_id,storage_node,active_log_lane,active_log_lanes,payload_bytes,total_nanos,admission_wait_nanos,stream_lock_wait_nanos,pending_lock_wait_nanos,active_log_lock_wait_nanos,metadata_prepare_nanos,metadata_record_nanos,payload_encode_nanos,payload_write_nanos,auto_persist_nanos,auto_persist_target_nanos,auto_persist_pending_nanos,auto_persist_sync_nanos,auto_persist_sync_file_nanos,auto_persist_sync_file_max_nanos,auto_persist_sync_dir_nanos,auto_persist_mark_nanos,auto_persist_request_nanos,auto_persist_wait_nanos,auto_persist_target_bytes,auto_persist_wait_target_bytes,auto_persist_pending_log_refs,auto_persist_pending_storage_nodes,auto_persist_sync_bytes,auto_persist_files_synced,auto_persist_sync_success,auto_persist_request_submitted,auto_persist_observed_synced_bytes,auto_persist_marked_bytes,background_sync_requested_bytes,background_sync_request_count,background_sync_step_bytes,max_in_flight_bytes,max_in_flight_bytes_per_storage_node,success";
    let mut file = open_csv_append(path, header)?;
    for profile in profiles {
        let row = [
            workload.name().to_string(),
            args.provider.to_string(),
            args.durability.to_string(),
            args.rtt.as_micros().to_string(),
            args.serial_rtts.to_string(),
            concurrency.to_string(),
            workload.op_size(args)?.to_string(),
            profile.sequence.to_string(),
            profile.stream_id.to_string(),
            profile.storage_node.to_string(),
            profile.active_log_lane.to_string(),
            profile.active_log_lanes.to_string(),
            profile.payload_bytes.to_string(),
            profile.total_nanos.to_string(),
            profile.admission_wait_nanos.to_string(),
            profile.stream_lock_wait_nanos.to_string(),
            profile.pending_lock_wait_nanos.to_string(),
            profile.active_log_lock_wait_nanos.to_string(),
            profile.metadata_prepare_nanos.to_string(),
            profile.metadata_record_nanos.to_string(),
            profile.payload_encode_nanos.to_string(),
            profile.payload_write_nanos.to_string(),
            profile.auto_persist_nanos.to_string(),
            profile.auto_persist_target_nanos.to_string(),
            profile.auto_persist_pending_nanos.to_string(),
            profile.auto_persist_sync_nanos.to_string(),
            profile.auto_persist_sync_file_nanos.to_string(),
            profile.auto_persist_sync_file_max_nanos.to_string(),
            profile.auto_persist_sync_dir_nanos.to_string(),
            profile.auto_persist_mark_nanos.to_string(),
            profile.auto_persist_request_nanos.to_string(),
            profile.auto_persist_wait_nanos.to_string(),
            profile.auto_persist_target_bytes.to_string(),
            profile.auto_persist_wait_target_bytes.to_string(),
            profile.auto_persist_pending_log_refs.to_string(),
            profile.auto_persist_pending_storage_nodes.to_string(),
            profile.auto_persist_sync_bytes.to_string(),
            profile.auto_persist_files_synced.to_string(),
            profile.auto_persist_sync_success.to_string(),
            profile.auto_persist_request_submitted.to_string(),
            profile.auto_persist_observed_synced_bytes.to_string(),
            profile.auto_persist_marked_bytes.to_string(),
            profile.background_sync_requested_bytes.to_string(),
            profile.background_sync_request_count.to_string(),
            profile.background_sync_step_bytes.to_string(),
            profile.max_in_flight_bytes.to_string(),
            profile.max_in_flight_bytes_per_storage_node.to_string(),
            profile.success.to_string(),
        ];
        writeln!(file, "{}", row.join(",")).map_err(fs_error)?;
    }
    Ok(())
}

fn append_append_log_profile_csv(args: &Args, report: &BenchReport) -> Result<()> {
    let Some(path) = &args.append_log_profile_csv else {
        return Ok(());
    };
    if report.append_log_profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,strategy,total_nanos,append_nanos,file_sync_nanos,file_sync_sum_nanos,file_sync_max_nanos,dir_sync_nanos,bytes_written,sync_bytes,append_record_count,estimated_run_count,files_synced,dirs_synced,storage_nodes,stream_count,max_file_bytes,target_data_log_bytes";
    let mut file = open_csv_append(path, header)?;
    for profile in &report.append_log_profiles {
        let row = [
            report.workload.name().to_string(),
            report.provider.to_string(),
            report.durability.to_string(),
            report.rtt_us.to_string(),
            report.serial_rtts.to_string(),
            report.concurrency.to_string(),
            report.op_size.to_string(),
            profile.strategy.to_string(),
            profile.total_nanos.to_string(),
            profile.append_nanos.to_string(),
            profile.file_sync_nanos.to_string(),
            profile.file_sync_sum_nanos.to_string(),
            profile.file_sync_max_nanos.to_string(),
            profile.dir_sync_nanos.to_string(),
            profile.bytes_written.to_string(),
            profile.sync_bytes.to_string(),
            profile.append_record_count.to_string(),
            profile.estimated_run_count.to_string(),
            profile.files_synced.to_string(),
            profile.dirs_synced.to_string(),
            profile.storage_nodes.to_string(),
            profile.stream_count.to_string(),
            profile.max_file_bytes.to_string(),
            profile.target_data_log_bytes.to_string(),
        ];
        writeln!(file, "{}", row.join(",")).map_err(fs_error)?;
    }
    Ok(())
}

fn append_metadata_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.metadata_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_metadata_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,sequence,phase,total_nanos,tx_lock_wait_nanos,read_validation_nanos,apply_write_nanos,commit_version_alloc_nanos,touched_key_shards,read_key_count,write_key_count,conflict_count";
    let mut file = open_csv_append(path, header)?;
    for profile in profiles {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            workload.name(),
            args.provider,
            args.durability,
            args.rtt.as_micros(),
            args.serial_rtts,
            concurrency,
            workload.op_size(args)?,
            profile.sequence,
            profile.phase,
            profile.total_nanos,
            profile.tx_lock_wait_nanos,
            profile.read_validation_nanos,
            profile.apply_write_nanos,
            profile.commit_version_alloc_nanos,
            profile.touched_key_shards,
            profile.read_key_count,
            profile.write_key_count,
            profile.conflict_count,
        )
        .map_err(fs_error)?;
    }
    Ok(())
}

fn append_block_write_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.block_write_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_block_write_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,storage_nodes,payload_integrity,sequence,total_nanos,device_spec_lookup_nanos,range_split_shard_head_read_nanos,write_intent_alloc_nanos,payload_copy_nanos,segment_write_nanos,storage_node_ids_nanos,placement_select_nanos,segment_id_alloc_nanos,grant_issue_nanos,storage_node_transport_dispatch_nanos,grant_verify_nanos,catalog_duplicate_probe_nanos,catalog_duplicate_probe_lock_wait_nanos,catalog_reserve_nanos,catalog_reserve_lock_wait_nanos,catalog_begin_nanos,catalog_begin_lock_wait_nanos,segment_store_write_nanos,segment_store_lock_wait_nanos,checksum_integrity_nanos,segment_store_insert_nanos,segment_sync_nanos,segment_sync_lock_wait_nanos,receipt_create_nanos,receipt_verify_nanos,catalog_commit_nanos,catalog_commit_lock_wait_nanos,tree_path_copy_nanos,metadata_publish_call_nanos,mark_referenced_nanos,mark_reference_evidence_nanos,mark_reference_transport_dispatch_nanos,mark_reference_verify_nanos,mark_reference_catalog_nanos,mark_reference_catalog_lock_wait_nanos,touched_shard_count,segment_count,profile_storage_node_count";
    let mut file = open_csv_append(path, header)?;
    for profile in profiles {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            workload.name(),
            args.provider,
            args.durability,
            args.rtt.as_micros(),
            args.serial_rtts,
            concurrency,
            workload.op_size(args)?,
            args.storage_nodes,
            payload_integrity_name(args.payload_integrity),
            profile.sequence,
            profile.total_nanos,
            profile.device_spec_lookup_nanos,
            profile.range_split_shard_head_read_nanos,
            profile.write_intent_alloc_nanos,
            profile.payload_copy_nanos,
            profile.segment_write_nanos,
            profile.storage_node_ids_nanos,
            profile.placement_select_nanos,
            profile.segment_id_alloc_nanos,
            profile.grant_issue_nanos,
            profile.storage_node_transport_dispatch_nanos,
            profile.grant_verify_nanos,
            profile.catalog_duplicate_probe_nanos,
            profile.catalog_duplicate_probe_lock_wait_nanos,
            profile.catalog_reserve_nanos,
            profile.catalog_reserve_lock_wait_nanos,
            profile.catalog_begin_nanos,
            profile.catalog_begin_lock_wait_nanos,
            profile.segment_store_write_nanos,
            profile.segment_store_lock_wait_nanos,
            profile.checksum_integrity_nanos,
            profile.segment_store_insert_nanos,
            profile.segment_sync_nanos,
            profile.segment_sync_lock_wait_nanos,
            profile.receipt_create_nanos,
            profile.receipt_verify_nanos,
            profile.catalog_commit_nanos,
            profile.catalog_commit_lock_wait_nanos,
            profile.tree_path_copy_nanos,
            profile.metadata_publish_call_nanos,
            profile.mark_referenced_nanos,
            profile.mark_reference_evidence_nanos,
            profile.mark_reference_transport_dispatch_nanos,
            profile.mark_reference_verify_nanos,
            profile.mark_reference_catalog_nanos,
            profile.mark_reference_catalog_lock_wait_nanos,
            profile.touched_shard_count,
            profile.segment_count,
            profile.storage_node_count,
        )
        .map_err(fs_error)?;
    }
    Ok(())
}

fn append_block_batch_profile_csv(
    args: &Args,
    report: &BenchReport,
) -> Result<()> {
    let Some(path) = &args.block_batch_profile_csv else {
        return Ok(());
    };
    if report.block_batch_profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,storage_nodes,payload_integrity,total_nanos,commit_nanos,flush_device_nanos,batch_operation_count,collapsed_range_count,requested_bytes,committed_bytes";
    let mut file = open_csv_append(path, header)?;
    for profile in &report.block_batch_profiles {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            report.workload.name(),
            report.provider,
            report.durability,
            report.rtt_us,
            report.serial_rtts,
            report.concurrency,
            report.op_size,
            args.storage_nodes,
            payload_integrity_name(args.payload_integrity),
            profile.total_nanos,
            profile.commit_nanos,
            profile.flush_device_nanos,
            profile.batch_operation_count,
            profile.collapsed_range_count,
            profile.requested_bytes,
            profile.committed_bytes,
        )
        .map_err(fs_error)?;
    }
    Ok(())
}

fn append_native_file_batch_profile_csv(
    args: &Args,
    report: &BenchReport,
) -> Result<()> {
    let Some(path) = &args.native_file_batch_profile_csv else {
        return Ok(());
    };
    if report.native_file_batch_profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,storage_nodes,payload_integrity,total_nanos,commit_nanos,batch_operation_count,requested_bytes,committed_range_bytes";
    let mut file = open_csv_append(path, header)?;
    for profile in &report.native_file_batch_profiles {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            report.workload.name(),
            report.provider,
            report.durability,
            report.rtt_us,
            report.serial_rtts,
            report.concurrency,
            report.op_size,
            args.storage_nodes,
            payload_integrity_name(args.payload_integrity),
            profile.total_nanos,
            profile.commit_nanos,
            profile.batch_operation_count,
            profile.requested_bytes,
            profile.committed_range_bytes,
        )
        .map_err(fs_error)?;
    }
    Ok(())
}

fn append_native_file_batch_commit_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.native_file_batch_commit_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_native_file_batch_commit_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,sequence,total_nanos,metadata_head_nanos,collapse_nanos,root_load_nanos,segment_group_nanos,preservation_check_nanos,preservation_read_nanos,overlay_nanos,segment_write_nanos,storage_node_ids_nanos,placement_select_nanos,segment_id_alloc_nanos,grant_issue_nanos,storage_node_transport_dispatch_nanos,grant_verify_nanos,catalog_duplicate_probe_nanos,catalog_duplicate_probe_lock_wait_nanos,catalog_reserve_nanos,catalog_reserve_lock_wait_nanos,catalog_begin_nanos,catalog_begin_lock_wait_nanos,segment_store_write_nanos,segment_store_lock_wait_nanos,checksum_integrity_nanos,segment_store_insert_nanos,segment_sync_nanos,segment_sync_lock_wait_nanos,receipt_create_nanos,receipt_verify_nanos,catalog_commit_nanos,catalog_commit_lock_wait_nanos,tree_path_copy_nanos,metadata_publish_nanos,mark_referenced_nanos,mark_reference_evidence_nanos,mark_reference_transport_dispatch_nanos,mark_reference_verify_nanos,mark_reference_catalog_nanos,mark_reference_catalog_lock_wait_nanos,append_stream_invalidate_nanos,write_count,collapsed_range_count,segment_group_count,segment_count,requested_bytes,committed_bytes,committed_range_bytes,preserved_read_bytes";
    let mut file = open_csv_append(path, header)?;
    for profile in profiles {
        let row = [
            workload.name().to_string(),
            args.provider.to_string(),
            args.durability.to_string(),
            args.rtt.as_micros().to_string(),
            args.serial_rtts.to_string(),
            concurrency.to_string(),
            workload.op_size(args)?.to_string(),
            profile.sequence.to_string(),
            profile.total_nanos.to_string(),
            profile.metadata_head_nanos.to_string(),
            profile.collapse_nanos.to_string(),
            profile.root_load_nanos.to_string(),
            profile.segment_group_nanos.to_string(),
            profile.preservation_check_nanos.to_string(),
            profile.preservation_read_nanos.to_string(),
            profile.overlay_nanos.to_string(),
            profile.segment_write_nanos.to_string(),
            profile.storage_node_ids_nanos.to_string(),
            profile.placement_select_nanos.to_string(),
            profile.segment_id_alloc_nanos.to_string(),
            profile.grant_issue_nanos.to_string(),
            profile.storage_node_transport_dispatch_nanos.to_string(),
            profile.grant_verify_nanos.to_string(),
            profile.catalog_duplicate_probe_nanos.to_string(),
            profile.catalog_duplicate_probe_lock_wait_nanos.to_string(),
            profile.catalog_reserve_nanos.to_string(),
            profile.catalog_reserve_lock_wait_nanos.to_string(),
            profile.catalog_begin_nanos.to_string(),
            profile.catalog_begin_lock_wait_nanos.to_string(),
            profile.segment_store_write_nanos.to_string(),
            profile.segment_store_lock_wait_nanos.to_string(),
            profile.checksum_integrity_nanos.to_string(),
            profile.segment_store_insert_nanos.to_string(),
            profile.segment_sync_nanos.to_string(),
            profile.segment_sync_lock_wait_nanos.to_string(),
            profile.receipt_create_nanos.to_string(),
            profile.receipt_verify_nanos.to_string(),
            profile.catalog_commit_nanos.to_string(),
            profile.catalog_commit_lock_wait_nanos.to_string(),
            profile.tree_path_copy_nanos.to_string(),
            profile.metadata_publish_nanos.to_string(),
            profile.mark_referenced_nanos.to_string(),
            profile.mark_reference_evidence_nanos.to_string(),
            profile.mark_reference_transport_dispatch_nanos.to_string(),
            profile.mark_reference_verify_nanos.to_string(),
            profile.mark_reference_catalog_nanos.to_string(),
            profile.mark_reference_catalog_lock_wait_nanos.to_string(),
            profile.append_stream_invalidate_nanos.to_string(),
            profile.write_count.to_string(),
            profile.collapsed_range_count.to_string(),
            profile.segment_group_count.to_string(),
            profile.segment_count.to_string(),
            profile.requested_bytes.to_string(),
            profile.committed_bytes.to_string(),
            profile.committed_range_bytes.to_string(),
            profile.preserved_read_bytes.to_string(),
        ];
        writeln!(file, "{}", row.join(",")).map_err(fs_error)?;
    }
    Ok(())
}

fn append_read_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.read_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_read_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    let header = "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,storage_nodes,payload_integrity,read_verification,sequence,total_nanos,metadata_resolve_nanos,metadata_lock_wait_nanos,metadata_tree_walk_nanos,metadata_placement_lookup_nanos,assemble_nanos,zero_fill_nanos,storage_node_read_nanos,storage_node_catalog_lookup_nanos,storage_node_payload_read_nanos,storage_node_lock_wait_nanos,verification_nanos,copy_nanos,block_journal_overlay_read_nanos,logical_bytes,extent_count,zero_extent_count,segment_extent_count,append_run_extent_count,profile_storage_node_count";
    let mut file = open_csv_append(path, header)?;
    for profile in profiles {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            workload.name(),
            args.provider,
            args.durability,
            args.rtt.as_micros(),
            args.serial_rtts,
            concurrency,
            workload.op_size(args)?,
            args.storage_nodes,
            payload_integrity_name(args.payload_integrity),
            read_verification_name(args.read_verification),
            profile.sequence,
            profile.total_nanos,
            profile.metadata_resolve_nanos,
            profile.metadata_lock_wait_nanos,
            profile.metadata_tree_walk_nanos,
            profile.metadata_placement_lookup_nanos,
            profile.assemble_nanos,
            profile.zero_fill_nanos,
            profile.storage_node_read_nanos,
            profile.storage_node_catalog_lookup_nanos,
            profile.storage_node_payload_read_nanos,
            profile.storage_node_lock_wait_nanos,
            profile.verification_nanos,
            profile.copy_nanos,
            profile.block_journal_overlay_read_nanos,
            profile.logical_bytes,
            profile.extent_count,
            profile.zero_extent_count,
            profile.segment_extent_count,
            profile.append_run_extent_count,
            profile.storage_node_count,
        )
        .map_err(fs_error)?;
    }
    Ok(())
}
