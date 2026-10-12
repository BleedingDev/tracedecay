"""Package an unpinned worker and expose its verified trust root to the host."""

def _ncm_worker_bundle_impl(ctx):
    toolchain = ctx.toolchains["@rules_rust//rust:toolchain_type"]
    if toolchain.target_triple == None:
        fail("NCM worker bundles require a named Rust target triple")
    target = toolchain.target_triple.str
    executable_name = "tracedecay-ncm-worker.exe" if "windows" in target else "tracedecay-ncm-worker"
    worker = ctx.actions.declare_file("worker-bundle/" + executable_name)
    manifest = ctx.actions.declare_file("worker-bundle/worker-manifest.json")
    model_manifest = ctx.actions.declare_file("worker-bundle/model-acquisition-manifest.json")
    ctx.actions.run(
        executable = ctx.executable._builder,
        arguments = [
            "--worker",
            ctx.file.worker.path,
            "--target",
            target,
            "--output",
            manifest.dirname,
            "--reference-manifest",
            ctx.file.reference_manifest.path,
            "--model-manifest",
            ctx.file.model_manifest.path,
        ],
        inputs = [ctx.file.worker, ctx.file.reference_manifest, ctx.file.model_manifest],
        outputs = [worker, manifest, model_manifest],
        use_default_shell_env = True,
        mnemonic = "NcmWorkerBundle",
        progress_message = "Pinning NCM worker for %s" % target,
    )
    return [
        DefaultInfo(files = depset([worker, manifest, model_manifest])),
        OutputGroupInfo(
            worker = depset([worker]),
            worker_manifest = depset([manifest]),
            model_acquisition_manifest = depset([model_manifest]),
        ),
    ]

ncm_worker_bundle = rule(
    implementation = _ncm_worker_bundle_impl,
    attrs = {
        "worker": attr.label(allow_single_file = True, mandatory = True),
        "reference_manifest": attr.label(allow_single_file = True, mandatory = True),
        "model_manifest": attr.label(allow_single_file = True, mandatory = True),
        "_builder": attr.label(
            default = "//scripts:product/ncm/build-worker-bundle.py",
            allow_single_file = True,
            executable = True,
            cfg = "exec",
        ),
    },
    toolchains = ["@rules_rust//rust:toolchain_type"],
)
